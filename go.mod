module d7y.io/dragonfly/v2

go 1.21

require (
	d7y.io/api/v2 v2.0.106
	github.com/MysteriousPotato/go-lockable v1.0.0
	github.com/RichardKnop/machinery v1.10.6
	github.com/Showmax/go-fqdn v1.0.0
	github.com/VividCortex/mysqlerr v1.0.0
	github.com/aliyun/aliyun-oss-go-sdk v3.0.2+incompatible
	github.com/appleboy/gin-jwt/v2 v2.9.2
	github.com/aws/aws-sdk-go v1.50.25
	github.com/bits-and-blooms/bitset v1.13.0
	github.com/casbin/casbin/v2 v2.81.0
	github.com/casbin/gorm-adapter/v3 v3.5.0
	github.com/colinmarc/hdfs/v2 v2.3.0
	github.com/containerd/containerd v1.6.26
	github.com/deckarep/golang-set/v2 v2.6.0
	github.com/distribution/distribution/v3 v3.0.0-20220620080156-3e4f8a0ab147
	github.com/docker/distribution v2.8.3+incompatible
	github.com/docker/docker v25.0.3+incompatible
	github.com/docker/go-connections v0.5.0
	github.com/docker/go-units v0.4.0
	github.com/gaius-qi/ping v1.0.0
	github.com/gammazero/deque v0.2.1
	github.com/gin-contrib/gzip v0.0.6
	github.com/gin-contrib/static v1.1.1
	github.com/gin-contrib/zap v1.1.0
	github.com/gin-gonic/gin v1.9.1
	github.com/go-echarts/statsview v0.3.4
	github.com/go-http-utils/headers v0.0.0-20181008091004-fed159eddc2a
	github.com/go-playground/validator/v10 v10.19.0
	github.com/go-redis/cache/v8 v8.4.3
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-redis/redismock/v8 v8.11.5
	github.com/go-sql-driver/mysql v1.7.1
	github.com/gocarina/gocsv v0.0.0-20221105105431-c8ef78125b99
	github.com/gofrs/flock v0.8.1
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/go-github v17.0.0+incompatible
	github.com/google/uuid v1.6.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/huaweicloud/huaweicloud-sdk-go-obs v3.23.12+incompatible
	github.com/jarcoal/httpmock v1.3.1
	github.com/johanbrandhorst/certify v1.9.0
	github.com/juju/ratelimit v1.0.2
	github.com/looplab/fsm v1.0.1
	github.com/mcuadros/go-gin-prometheus v0.1.0
	github.com/mdlayher/vsock v1.2.1
	github.com/mitchellh/mapstructure v1.5.0
	github.com/montanaflynn/stats v0.7.1
	github.com/onsi/ginkgo/v2 v2.16.0
	github.com/onsi/gomega v1.32.0
	github.com/opencontainers/image-spec v1.1.0
	github.com/orcaman/concurrent-map/v2 v2.0.1
	github.com/phayes/freeport v0.0.0-20220201140144-74d24b5ae9f5
	github.com/prometheus/client_golang v1.19.0
	github.com/schollz/progressbar/v3 v3.14.2
	github.com/shirou/gopsutil/v3 v3.23.12
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.7.0
	github.com/spf13/viper v1.18.2
	github.com/stretchr/testify v1.9.0
	github.com/swaggo/files v1.0.1
	github.com/swaggo/gin-swagger v1.6.0
	github.com/swaggo/swag v1.16.3
	github.com/yl2chen/cidranger v1.0.2
	go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v0.47.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0
	go.opentelemetry.io/otel v1.24.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/sdk v1.24.0
	go.opentelemetry.io/otel/trace v1.24.0
	go.uber.org/atomic v1.11.0
	go.uber.org/mock v0.4.0
	go.uber.org/zap v1.27.0
	golang.org/x/crypto v0.21.0
	golang.org/x/exp v0.0.0-20230905200255-921286631fa9
	golang.org/x/oauth2 v0.18.0
	golang.org/x/sync v0.6.0
	golang.org/x/sys v0.18.0
	golang.org/x/time v0.5.0
	google.golang.org/api v0.171.0
	google.golang.org/grpc v1.62.1
	google.golang.org/protobuf v1.33.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v3 v3.0.1
	gorm.io/driver/mysql v1.5.4
	gorm.io/driver/postgres v1.5.7
	gorm.io/gorm v1.25.8
	gorm.io/plugin/soft_delete v1.2.1
	k8s.io/component-base v0.29.2
	logur.dev/adapter/zap v0.5.0
	moul.io/zapgorm2 v1.3.0
	stathat.com/c/consistent v1.0.0
)

require (
	cloud.google.com/go v0.112.0 // indirect
	cloud.google.com/go/compute v1.23.4 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.6 // indirect
	cloud.google.com/go/pubsub v1.36.1 // indirect
	github.com/KyleBanks/depth v1.2.1 // indirect
	github.com/Microsoft/hcsshim v0.9.10 // indirect
	github.com/RichardKnop/logging v0.0.0-20190827224416-1a693bdd4fae // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/bradfitz/gomemcache v0.0.0-20220106215444-fb4bf637b56d // indirect
	github.com/bytedance/sonic v1.11.3 // indirect
	github.com/casbin/govaluate v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/chenzhuoyu/base64x v0.0.0-20230717121745-296ad89f973d // indirect
	github.com/chenzhuoyu/iasm v0.9.1 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/distribution/reference v0.5.0 // indirect
	github.com/docker/go-metrics v0.0.1 // indirect
	github.com/docker/libtrust v0.0.0-20150114040149-fa567046d9b1 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.0.4 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.3 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-echarts/go-echarts/v2 v2.2.4 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/jsonpointer v0.19.6 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/spec v0.20.6 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-redsync/redsync/v4 v4.5.1 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.0 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/pprof v0.0.0-20221118152302-e6195bd50e26 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.3 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgx/v5 v5.4.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.2 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kelseyhightower/envconfig v1.4.0 // indirect
	github.com/klauspost/compress v1.17.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.7 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-sqlite3 v1.14.15 // indirect
	github.com/mdlayher/socket v0.4.1 // indirect
	github.com/microsoft/go-mssqldb v0.17.0 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pelletier/go-toml/v2 v2.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20220216144756-c35f1ee13d7c // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common v0.48.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rs/cors v1.8.2 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sagikazarmark/locafero v0.4.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/cast v1.6.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/streadway/amqp v1.0.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.2.12 // indirect
	github.com/vmihailenco/go-tinylfu v0.2.2 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.0.2 // indirect
	github.com/xdg-go/stringprep v1.0.2 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	go.mongodb.org/mongo-driver v1.9.1 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/arch v0.7.0 // indirect
	golang.org/x/net v0.22.0 // indirect
	golang.org/x/term v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/tools v0.17.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20240205150955-31a09d347014 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240311132316-a219d84964c2 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240314234333-6e1732d8331c // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gorm.io/driver/sqlserver v1.4.1 // indirect
	gorm.io/plugin/dbresolver v1.3.0 // indirect
	k8s.io/apimachinery v0.29.2 // indirect
	k8s.io/klog/v2 v2.110.1 // indirect
	logur.dev/logur v0.16.1 // indirect
)
