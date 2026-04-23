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

	// Multi-space mode. When --space name=path entries are supplied, wrap
	// the default stack's server + each extra space behind a Manager that
	// routes /api/kiwi/{space}/... to the right backend. The default space
	// is registered under "default" (first in order → fallback for
	// non-prefixed requests) so single-space clients keep working.
	var (
		extraHandler http.Handler
		spaceMgr     *spaces.Manager
	)
	if len(spaceSpecs) > 0 {
		spaceMgr = spaces.NewManager(cfg)
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
		extraHandler = spaceMgr.Handler()
	}

	if wantS3, _ := cmd.Flags().GetBool("s3"); wantS3 {
		port, _ := cmd.Flags().GetInt("s3-port")
		s3Addr := fmt.Sprintf("%s:%d", cfg.Server.Host, port)
		var s3Srv *kiwis3.Server
		if spaceMgr != nil {
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

	if extraHandler != nil {
		log.Printf("KiwiFS multi-space serving on http://%s", addr)
		runHTTP("KiwiFS multi-space", &http.Server{Addr: addr, Handler: extraHandler})
	} else {
		log.Printf("KiwiFS serving %s on http://%s", root, addr)
		srv := stack.Server
		g.Go(func() error {
			if err := srv.Start(addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("rest: %w", err)
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
