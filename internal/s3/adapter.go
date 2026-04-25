package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/storage"
)

// defaultBucket is the single bucket name exposed when no per-space
// buckets are registered. Single-space deployments stay on this name
// (matches the previous hand-rolled implementation, so awscli scripts
// that hardcoded `s3://knowledge/` keep working).
const defaultBucket = "knowledge"

// SpaceBackend bundles the per-space pipeline and storage so the adapter
// can dispatch by bucket name. Each entry corresponds to one
// independently-built bootstrap.Stack — separate git repo, search
// index, and (where configured) vector store.
type SpaceBackend struct {
	Store storage.Storage
	Pipe  *pipeline.Pipeline
}

// adapter implements gofakes3.Backend on top of one or more KiwiFS
// spaces. Each space appears as a top-level S3 bucket; the adapter
// looks up the right (store, pipe) pair before dispatching reads or
// pipeline writes. Single-space deployments register one bucket
// (`knowledge`) and behave exactly like the previous implementation.
//
// Read paths (Get/Head/List) hit storage directly; the pipeline is a
// write-side construct.
type adapter struct {
	buckets   map[string]SpaceBackend
	order     []string // registration order so ListBuckets is deterministic
	startedAt time.Time
}

// newAdapter constructs the Backend with one or more named buckets.
// There's no per-call ctx in the gofakes3.Backend interface, so each
// method synthesises a Background ctx for downstream calls — when
// gofakes3 grows ctx-aware methods we'll thread the request ctx
// through here.
func newAdapter(buckets map[string]SpaceBackend, order []string) *adapter {
	return &adapter{
		buckets:   buckets,
		order:     order,
		startedAt: time.Now(),
	}
}

// resolve returns the backend for `name` or BucketNotFound if no space
// is registered under that bucket.
func (a *adapter) resolve(name string) (SpaceBackend, error) {
	be, ok := a.buckets[name]
	if !ok {
		return SpaceBackend{}, gofakes3.BucketNotFound(name)
	}
	return be, nil
}

func (a *adapter) ListBuckets() ([]gofakes3.BucketInfo, error) {
	out := make([]gofakes3.BucketInfo, 0, len(a.order))
	for _, name := range a.order {
		out = append(out, gofakes3.BucketInfo{
			Name:         name,
			CreationDate: gofakes3.NewContentTime(a.startedAt),
		})
	}
	return out, nil
}

func (a *adapter) BucketExists(name string) (bool, error) {
	_, ok := a.buckets[name]
	return ok, nil
}

// CreateBucket / DeleteBucket / ForceDeleteBucket are intentionally not
// supported — KiwiFS exposes one fixed bucket backed by the knowledge
// root; bucket lifecycle is a deployment concern, not an S3 client one.
// Returning ErrNotImplemented (rather than silently succeeding) makes the
// constraint visible to clients that try.
func (a *adapter) CreateBucket(_ string) error {
	return gofakes3.ErrNotImplemented
}

func (a *adapter) DeleteBucket(_ string) error {
	return gofakes3.ErrNotImplemented
}

func (a *adapter) ForceDeleteBucket(_ string) error {
	return gofakes3.ErrNotImplemented
}

// ListBucket walks the root with prefix/delimiter filtering and pagination.
func (a *adapter) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	be, err := a.resolve(name)
	if err != nil {
		return nil, err
	}
	if prefix == nil {
		prefix = &gofakes3.Prefix{}
	}
	out := gofakes3.NewObjectList()

	maxKeys := page.MaxKeys
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}

	// Walk the entire tree; gofakes3 expects the backend to do its own
	// prefix matching. The walk is bounded by the knowledge tree size,
	// not request shape — pagination above caps the response.
	ctx := context.Background()
	var walkErr error
	count := int64(0)
	walkErr = walk(ctx, be.Store, "/", func(e storage.Entry) error {
		key := strings.TrimPrefix(e.Path, "/")

		// Marker pagination: skip everything ≤ marker. Walk order is
		// lexical (storage.Walk delegates to filepath.WalkDir
		// eventually), which matches S3's UTF-8 sort order well enough
		// for the single-bucket case.
		if page.HasMarker && key <= page.Marker {
			return nil
		}

		var match gofakes3.PrefixMatch
		if !prefix.Match(key, &match) {
			return nil
		}
		if match.CommonPrefix {
			out.AddPrefix(match.MatchedPart)
			return nil
		}

		etag := weakETag(e.Size, e.ModTime)
		out.Add(&gofakes3.Content{
			Key:          key,
			LastModified: gofakes3.NewContentTime(e.ModTime),
			ETag:         `"` + etag + `"`,
			Size:         e.Size,
			StorageClass: gofakes3.StorageStandard,
		})
		count++
		if count >= maxKeys {
			out.IsTruncated = true
			out.NextMarker = key
			return errStopWalk
		}
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, errStopWalk) {
		return nil, walkErr
	}
	return out, nil
}

// errStopWalk lets the walker break out cleanly when the page is full.
var errStopWalk = errors.New("s3 walker: page filled")

// walk is a thin wrapper around storage.Walk that returns directories too
// (S3 has no directory concept, but gofakes3's prefix matcher needs
// every key it might fold into CommonPrefixes — which means *every* file
// under a prefix). For now this is the same as storage.Walk because
// directories are never yielded as Content; the function exists so a
// future "show empty dirs" toggle has a single place to land.
func walk(ctx context.Context, store storage.Storage, root string, fn storage.WalkFunc) error {
	return storage.Walk(ctx, store, root, fn)
}

func (a *adapter) GetObject(bucket, key string, _ *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	be, err := a.resolve(bucket)
	if err != nil {
		return nil, err
	}
	data, err := be.Store.Read(context.Background(), key)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, gofakes3.KeyNotFound(key)
		}
		return nil, err
	}
	return &gofakes3.Object{
		Name:     key,
		Metadata: map[string]string{"Content-Type": contentTypeFor(key, data)},
		Size:     int64(len(data)),
		// Hash is the body MD5 — gofakes3 formats it as the ETag header
		// for response framing. We don't store git-blob ETags here
		// because clients that GET → PUT-back use the value verbatim,
		// and an MD5 mismatch on a re-upload would surface as a noisy
		// (but harmless) integrity warning in some S3 SDKs.
		Hash:     md5sum(data),
		Contents: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (a *adapter) HeadObject(bucket, key string) (*gofakes3.Object, error) {
	be, err := a.resolve(bucket)
	if err != nil {
		return nil, err
	}
	info, err := be.Store.Stat(context.Background(), key)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, gofakes3.KeyNotFound(key)
		}
		return nil, err
	}
	// HEAD must NOT read the body — that would defeat the point of the
	// verb on large files. Use a weak size+mtime ETag (same shape as the
	// list response) so range/cache validators stay consistent across
	// HEAD and LIST.
	etag := weakETag(info.Size, info.ModTime)
	return &gofakes3.Object{
		Name: key,
		Metadata: map[string]string{
			"Content-Type":  contentTypeFor(key, nil),
			"Last-Modified": info.ModTime.UTC().Format(http.TimeFormat),
		},
		Size:     info.Size,
		Hash:     []byte(etag),
		Contents: io.NopCloser(strings.NewReader("")),
	}, nil
}

// s3StreamThreshold picks the point at which we route a PUT through the
// streaming pipeline path rather than buffering the whole body in RAM.
const s3StreamThreshold = pipeline.StreamInMemoryThreshold

func (a *adapter) PutObject(bucket, key string, meta map[string]string, body io.Reader, size int64, conditions *gofakes3.PutConditions) (gofakes3.PutObjectResult, error) {
	be, err := a.resolve(bucket)
	if err != nil {
		return gofakes3.PutObjectResult{}, err
	}
	actor := meta["X-Actor"]
	if actor == "" {
		actor = "s3"
	}
	// Fast path for small / size-known uploads: buffer in RAM, run the
	// full precondition check, and call pipeline.Write. This matches the
	// REST handler byte-for-byte.
	if conditions != nil || (size >= 0 && size <= s3StreamThreshold) {
		data, rerr := gofakes3.ReadAll(body, size)
		if rerr != nil {
			return gofakes3.PutObjectResult{}, rerr
		}
		if conditions != nil {
			info := gofakes3.ConditionalObjectInfo{}
			if existing, err := be.Store.Read(context.Background(), key); err == nil {
				info.Exists = true
				info.Hash = md5sum(existing)
			}
			if cerr := gofakes3.CheckPutConditions(conditions, &info); cerr != nil {
				return gofakes3.PutObjectResult{}, cerr
			}
		}
		if _, werr := be.Pipe.Write(context.Background(), key, data, actor); werr != nil {
			log.Printf("s3: put %s/%s: %v", bucket, key, werr)
			return gofakes3.PutObjectResult{}, werr
		}
		return gofakes3.PutObjectResult{}, nil
	}
	// Large or unknown-size upload: stream straight through. The
	// pipeline spools to a tempfile in the store's root, renames into
	// place, and commits — all without loading the payload into memory.
	if _, werr := be.Pipe.WriteStream(context.Background(), key, body, size, actor); werr != nil {
		log.Printf("s3: stream put %s/%s: %v", bucket, key, werr)
		return gofakes3.PutObjectResult{}, werr
	}
	return gofakes3.PutObjectResult{}, nil
}

func (a *adapter) DeleteObject(bucket, key string) (gofakes3.ObjectDeleteResult, error) {
	be, err := a.resolve(bucket)
	if err != nil {
		return gofakes3.ObjectDeleteResult{}, err
	}
	if err := be.Pipe.Delete(context.Background(), key, "s3"); err != nil {
		// S3 DELETE is idempotent: missing key returns 204, not 404.
		// Real I/O errors do propagate so a broken pipeline doesn't
		// silently drop deletes.
		if os.IsNotExist(err) {
			return gofakes3.ObjectDeleteResult{}, nil
		}
		log.Printf("s3: delete %s/%s: %v", bucket, key, err)
		return gofakes3.ObjectDeleteResult{}, err
	}
	return gofakes3.ObjectDeleteResult{}, nil
}

func (a *adapter) DeleteMulti(bucket string, keys ...string) (gofakes3.MultiDeleteResult, error) {
	res := gofakes3.MultiDeleteResult{}
	for _, k := range keys {
		if _, err := a.DeleteObject(bucket, k); err != nil {
			res.Error = append(res.Error, gofakes3.ErrorResult{
				Key:     k,
				Code:    gofakes3.ErrInternal,
				Message: err.Error(),
			})
			continue
		}
		res.Deleted = append(res.Deleted, gofakes3.ObjectID{Key: k})
	}
	return res, nil
}

func (a *adapter) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, _ map[string]string) (gofakes3.CopyObjectResult, error) {
	src, err := a.resolve(srcBucket)
	if err != nil {
		return gofakes3.CopyObjectResult{}, err
	}
	dst, err := a.resolve(dstBucket)
	if err != nil {
		return gofakes3.CopyObjectResult{}, err
	}
	data, err := src.Store.Read(context.Background(), srcKey)
	if err != nil {
		if os.IsNotExist(err) {
			return gofakes3.CopyObjectResult{}, gofakes3.KeyNotFound(srcKey)
		}
		return gofakes3.CopyObjectResult{}, err
	}
	res, err := dst.Pipe.Write(context.Background(), dstKey, data, "s3")
	if err != nil {
		return gofakes3.CopyObjectResult{}, err
	}
	return gofakes3.CopyObjectResult{
		ETag:         `"` + res.ETag + `"`,
		LastModified: gofakes3.NewContentTime(time.Now().UTC()),
	}, nil
}

// md5sum returns the MD5 digest of data. Used as the body hash on GET
// responses and the equality check input for conditional writes — both
// match what real S3 does for unencrypted objects.
func md5sum(data []byte) []byte {
	h := md5.Sum(data)
	out := make([]byte, hex.DecodedLen(len(h)*2))
	copy(out, h[:])
	return out[:len(h)]
}
