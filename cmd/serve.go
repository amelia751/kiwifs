package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kiwifs/kiwifs/internal/backup"
	"github.com/kiwifs/kiwifs/internal/bootstrap"
	"github.com/kiwifs/kiwifs/internal/config"
	kiwinfs "github.com/kiwifs/kiwifs/internal/nfs"
	kiwis3 "github.com/kiwifs/kiwifs/internal/s3"
	"github.com/kiwifs/kiwifs/internal/spaces"
	"github.com/kiwifs/kiwifs/internal/watcher"
	kiwidav "github.com/kiwifs/kiwifs/internal/webdav"
	"github.com/spf13/cobra"
	nfs "github.com/willscott/go-nfs"
	"golang.org/x/sync/errgroup"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the KiwiFS server",
	Example: `  kiwifs serve --root ~/my-knowledge --port 3333
  kiwifs serve --root /data/knowledge --port 3333 --host 0.0.0.0`,
	RunE: runServe,
}

func init() {
	serveCmd.Flags().StringP("root", "r", "./knowledge", "knowledge root directory")
	serveCmd.Flags().IntP("port", "p", 3333, "HTTP port")
	serveCmd.Flags().String("host", "0.0.0.0", "bind address")
	serveCmd.Flags().String("search", "sqlite", "search engine: sqlite | grep")
	serveCmd.Flags().String("versioning", "git", "versioning strategy: git | cow | none")
	serveCmd.Flags().String("auth", "none", "auth type: none | apikey | perspace | oidc")
	serveCmd.Flags().String("api-key", "", "API key (required if auth=apikey)")
	serveCmd.Flags().String("oidc-issuer", "", "OIDC provider issuer URL (required if auth=oidc)")
	serveCmd.Flags().String("oidc-client-id", "", "OIDC client ID (required if auth=oidc)")
	serveCmd.Flags().Bool("async-commit", true, "enable async batched git commits (default true)")
	serveCmd.Flags().Int("batch-window", 200, "async commit batch window in milliseconds")
	serveCmd.Flags().Int("batch-max-size", 50, "max paths per async commit batch")
	serveCmd.Flags().Bool("no-watch", false, "disable the fsnotify watcher that catches direct writes to --root")
	serveCmd.Flags().Bool("webdav", false, "enable the WebDAV server alongside the REST API")
	serveCmd.Flags().Int("webdav-port", 3335, "WebDAV listen port (used when --webdav is set)")
	serveCmd.Flags().Bool("nfs", false, "enable the NFS server (userspace NFSv3 for Docker/K8s native mounts)")
	serveCmd.Flags().Int("nfs-port", 2049, "NFS listen port (used when --nfs is set)")
	serveCmd.Flags().String("nfs-allow", "", "comma-separated CIDRs allowed to mount NFS (default: localhost only)")
	serveCmd.Flags().Bool("s3", false, "enable the S3-compatible API (aws cli, boto3, rclone)")
	serveCmd.Flags().Int("s3-port", 3334, "S3 API listen port (used when --s3 is set)")
	serveCmd.Flags().StringSlice("space", nil, "register an additional space (repeatable, format: name=path). Enables multi-space routing at /api/kiwi/{name}/...")
}

func runServe(cmd *cobra.Command, args []string) error {
	root, _ := cmd.Flags().GetString("root")

	// Auto-init: if root has no .kiwi/config.toml, initialize it.
	kiwiConfig := fmt.Sprintf("%s/.kiwi/config.toml", root)
	if _, err := os.Stat(kiwiConfig); os.IsNotExist(err) {
		log.Printf("No config found at %s — auto-initializing...", root)
		initCmd.Flags().Set("root", root)
		if err := runInit(initCmd, nil); err != nil {
			return fmt.Errorf("auto-init: %w", err)
		}
	}

	// Load TOML config as base; CLI flags that were explicitly set override it.
	cfg, err := config.Load(root)
	if err != nil {
		log.Printf("warning: could not load config.toml (%v) — using defaults", err)
		cfg = &config.Config{}
	}

	applyFlag := func(name string, apply func()) {
		if cmd.Flags().Changed(name) {
			apply()
		}
	}

	applyFlag("port", func() { cfg.Server.Port, _ = cmd.Flags().GetInt("port") })
	applyFlag("host", func() { cfg.Server.Host, _ = cmd.Flags().GetString("host") })
	applyFlag("search", func() { cfg.Search.Engine, _ = cmd.Flags().GetString("search") })
	applyFlag("versioning", func() { cfg.Versioning.Strategy, _ = cmd.Flags().GetString("versioning") })
	applyFlag("async-commit", func() {
		v, _ := cmd.Flags().GetBool("async-commit")
		cfg.Versioning.AsyncCommit = &v
	})
	applyFlag("batch-window", func() { cfg.Versioning.BatchWindowMs, _ = cmd.Flags().GetInt("batch-window") })
	applyFlag("batch-max-size", func() { cfg.Versioning.BatchMaxSize, _ = cmd.Flags().GetInt("batch-max-size") })
	applyFlag("auth", func() { cfg.Auth.Type, _ = cmd.Flags().GetString("auth") })
	applyFlag("api-key", func() { cfg.Auth.APIKey, _ = cmd.Flags().GetString("api-key") })
	applyFlag("oidc-issuer", func() { cfg.Auth.OIDC.Issuer, _ = cmd.Flags().GetString("oidc-issuer") })
	applyFlag("oidc-client-id", func() { cfg.Auth.OIDC.ClientID, _ = cmd.Flags().GetString("oidc-client-id") })

	// Apply flag defaults for fields still unset after TOML load.
	if cfg.Server.Port == 0 {
		cfg.Server.Port, _ = cmd.Flags().GetInt("port")
	}
	if cfg.Server.Host == "" {
		cfg.Server.Host, _ = cmd.Flags().GetString("host")
	}
	if cfg.Search.Engine == "" {
		cfg.Search.Engine, _ = cmd.Flags().GetString("search")
	}
	if cfg.Versioning.Strategy == "" {
		cfg.Versioning.Strategy, _ = cmd.Flags().GetString("versioning")
	}
	if cfg.Auth.Type == "" {
		cfg.Auth.Type, _ = cmd.Flags().GetString("auth")
	}
	cfg.Storage.Root = root

	noWatch, _ := cmd.Flags().GetBool("no-watch")

	// In multi-space mode, filter API keys so each space's server only
	// accepts keys scoped to that space. Applied before building the
	// default stack so its auth middleware is correctly configured.
	spaceSpecs, _ := cmd.Flags().GetStringSlice("space")
	defaultCfg := cfg
	if len(spaceSpecs) > 0 {
		defaultCfg = spaces.FilterKeysForSpace(cfg, "default")
	}
	stack, err := bootstrap.Build("default", root, defaultCfg)
	if err != nil {
		return err
	}
	defer stack.Close()

	// Recover any paths that were written to disk but not committed
	// before a previous crash. Must run before the watcher starts so
	// the git audit trail has no silent gaps.
	stack.Pipeline.DrainUncommitted(context.Background())

	if !noWatch {
		w, werr := watcher.New(root, stack.Store, stack.Pipeline)
		if werr != nil {
			log.Printf("warning: fsnotify watcher unavailable (%v) — running without it", werr)
		} else {
			w.Start()
			defer w.Close()
			log.Printf("fsnotify watcher: watching %s for direct .md writes", root)
		}
	}

	if cfg.Backup.Remote != "" {
		syncer, berr := backup.New(root, cfg.Backup.Remote, cfg.Backup.Branch, cfg.Backup.Interval)
		if berr != nil {
			log.Printf("warning: backup sync disabled (%v)", berr)
		} else {
			syncer.Start()
			defer syncer.Close()
		}
	}

	// Root context wired to SIGINT / SIGTERM. All servers derive from this,
	// so a single Ctrl-C fans out shutdown across REST, WebDAV, NFS, and S3
	// instead of leaving any of them half-serving during process exit.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	g, gctx := errgroup.WithContext(ctx)

	const shutdownTimeout = 15 * time.Second

	// runHTTP wires an http.Server under the errgroup so every alt-protocol
	// HTTP endpoint drains on shutdown signal with the same policy. label
	// only exists for logging.
	runHTTP := func(label string, srv *http.Server) {
		g.Go(func() error {
			log.Printf("%s listening on http://%s", label, srv.Addr)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("%s: %w", label, err)
			}
			return nil
		})
		g.Go(func() error {
			<-gctx.Done()
			shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			_ = srv.Shutdown(shutCtx)
			return nil
		})
	}

	if wantWebDAV, _ := cmd.Flags().GetBool("webdav"); wantWebDAV {
		port, _ := cmd.Flags().GetInt("webdav-port")
		addr := fmt.Sprintf("%s:%d", cfg.Server.Host, port)
		handler := kiwidav.New(root, stack.Pipeline, "webdav", cfg.Auth.APIKey).Handler("")
		runHTTP("KiwiFS WebDAV", &http.Server{Addr: addr, Handler: handler})
	}

	if wantNFS, _ := cmd.Flags().GetBool("nfs"); wantNFS {
		port, _ := cmd.Flags().GetInt("nfs-port")
		allowSpec, _ := cmd.Flags().GetString("nfs-allow")
		allow, aerr := kiwinfs.ParseAllow(allowSpec)
		if aerr != nil {
			return fmt.Errorf("parse --nfs-allow: %w", aerr)
		}
		nfsSrv, nerr := kiwinfs.New(root, stack.Pipeline, allow)
		if nerr != nil {
			log.Printf("warning: NFS server init failed (%v) — skipping NFS", nerr)
		} else {
			addr := fmt.Sprintf("%s:%d", cfg.Server.Host, port)
			listener, nerr := net.Listen("tcp", addr)
			if nerr != nil {
				log.Printf("warning: NFS listener failed (%v) — skipping NFS", nerr)
			} else {
				handler := nfsSrv.Handler()
				g.Go(func() error {
					log.Printf("KiwiFS NFS listening on nfs://%s", addr)
					log.Printf("  Docker: docker run --mount type=nfs,source=/,target=/mnt/kiwi,o=addr=%s ...", cfg.Server.Host)
					log.Printf("  macOS:  mount -t nfs %s:/ /mnt/kiwi", cfg.Server.Host)
					err := nfs.Serve(listener, handler)
					// go-nfs returns an error when the listener closes;
					// treat that as a clean shutdown rather than a fatal
					// signal that would take the whole process down.
					if err != nil && !errors.Is(err, net.ErrClosed) {
						return fmt.Errorf("nfs: %w", err)
					}
					return nil
				})
				g.Go(func() error {
					<-gctx.Done()
					// go-nfs has no Shutdown — closing the listener is the
					// only way to stop nfs.Serve from accepting new conns.
					_ = listener.Close()
					return nil
				})
			}
		}
	}

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)

	// Always create a Manager so /api/spaces is available even in
	// single-space mode. The default space is registered first (fallback
	// for non-prefixed requests) so existing clients keep working.
	spaceMgr := spaces.NewManager(cfg)
	if err := spaceMgr.RegisterStack("default", root, stack); err != nil {
		return fmt.Errorf("register default space: %w", err)
	}
	for _, spec := range spaceSpecs {
		name, spacePath, ok := strings.Cut(spec, "=")
		name = strings.TrimSpace(name)
		spacePath = strings.TrimSpace(spacePath)
		if !ok || name == "" || spacePath == "" {
			return fmt.Errorf("invalid --space %q (want name=path)", spec)
		}
		sub := *cfg
		sub.Storage.Root = spacePath
		filtered := spaces.FilterKeysForSpace(&sub, name)
		if err := spaceMgr.AddSpace(name, spacePath, filtered); err != nil {
			return fmt.Errorf("add space %q: %w", name, err)
		}
		log.Printf("space %q mounted at /api/kiwi/%s → %s", name, name, spacePath)
	}
	defer spaceMgr.Close()
	mgrHandler := spaceMgr.Handler()

	if wantS3, _ := cmd.Flags().GetBool("s3"); wantS3 {
		port, _ := cmd.Flags().GetInt("s3-port")
		s3Addr := fmt.Sprintf("%s:%d", cfg.Server.Host, port)
		var s3Srv *kiwis3.Server
		if len(spaceSpecs) > 0 {
			// One bucket per space. Order matches Manager.ListSpaces so
			// awscli sees buckets in the same order they were registered.
			buckets := map[string]kiwis3.SpaceBackend{}
			order := spaceMgr.ListSpaces()
			for _, name := range order {
				sp, _ := spaceMgr.GetSpace(name)
				if sp == nil || sp.Stack == nil {
					continue
				}
				buckets[name] = kiwis3.SpaceBackend{
					Store: sp.Stack.Store,
					Pipe:  sp.Stack.Pipeline,
				}
			}
			s3Srv = kiwis3.NewMultiSpace(buckets, order, cfg.Auth.APIKey)
			for _, name := range order {
				log.Printf("  aws s3 ls s3://%s/ --endpoint-url http://%s:%d", name, cfg.Server.Host, port)
			}
		} else {
			s3Srv = kiwis3.New(root, stack.Pipeline, stack.Store, cfg.Auth.APIKey)
			log.Printf("  aws s3 ls s3://knowledge/ --endpoint-url http://%s:%d", cfg.Server.Host, port)
			log.Printf("  aws s3 cp file.md s3://knowledge/path/ --endpoint-url http://%s:%d", cfg.Server.Host, port)
		}
		runHTTP("KiwiFS S3 API", &http.Server{Addr: s3Addr, Handler: s3Srv.Handler()})
	}

	if len(spaceSpecs) > 0 {
		log.Printf("KiwiFS multi-space serving on http://%s", addr)
	} else {
		log.Printf("KiwiFS serving %s on http://%s", root, addr)
	}
	runHTTP("KiwiFS", &http.Server{Addr: addr, Handler: mgrHandler})

	// SIGHUP → hot-reload auth from disk. Matches how nginx, haproxy, and
	// Postgres reload their key material; operators rotating an API key
	// can now do so without dropping an in-flight upload. We only swap
	// auth config (safe behind an atomic pointer inside api.Server) —
	// port changes / versioner swaps still require a restart because
	// those are structural.
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	g.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				signal.Stop(sighup)
				return nil
			case <-sighup:
				newCfg, err := config.Load(root)
				if err != nil {
					log.Printf("sighup: config reload failed (%v) — keeping old keys", err)
					continue
				}
				for _, name := range spaceMgr.ListSpaces() {
					sp, ok := spaceMgr.GetSpace(name)
					if !ok || sp == nil || sp.Server == nil {
						continue
					}
					// Each space sees a filtered subset of the global key
					// list (per-space keys only apply to their own root),
					// so use the same filter path as AddSpace.
					filtered := spaces.FilterKeysForSpace(newCfg, name)
					sp.Server.ReloadAuth(&filtered.Auth)
				}
				log.Printf("sighup: auth reloaded for %d space(s)", len(spaceMgr.ListSpaces()))
			}
		}
	})

	// Signal-triggered context cancellation propagates through gctx once
	// any server returns an error OR the signal fires. Wait returns the
	// first non-nil error — or nil on a clean shutdown.
	err = g.Wait()
	// A caller-triggered signal produces ctx.Err() == context.Canceled;
	// don't treat that as a runtime failure.
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Printf("shutdown complete")
	return nil
}
