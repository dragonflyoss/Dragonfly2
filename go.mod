module d7y.io/dragonfly/v2

go 1.17

require (
	github.com/RichardKnop/machinery v1.10.6
	github.com/Showmax/go-fqdn v1.0.0
	github.com/VividCortex/mysqlerr v1.0.0
	github.com/agiledragon/gomonkey/v2 v2.3.1
	github.com/aliyun/aliyun-oss-go-sdk v2.1.6+incompatible
	github.com/appleboy/gin-jwt/v2 v2.6.5-0.20210827121450-79689222c755
	github.com/bits-and-blooms/bitset v1.2.1
	github.com/casbin/casbin/v2 v2.34.1
	github.com/casbin/gorm-adapter/v3 v3.3.2
	github.com/colinmarc/hdfs/v2 v2.2.0
	github.com/distribution/distribution/v3 v3.0.0-20210804104954-38ab4c606ee3
	github.com/docker/go-connections v0.4.0
	github.com/docker/go-units v0.4.0
	github.com/emirpasic/gods v1.12.0
	github.com/envoyproxy/protoc-gen-validate v0.6.1
	github.com/gin-contrib/cors v1.3.1
	github.com/gin-contrib/static v0.0.1
	github.com/gin-gonic/gin v1.7.7
	github.com/go-echarts/statsview v0.3.4
	github.com/go-http-utils/headers v0.0.0-20181008091004-fed159eddc2a
	github.com/go-playground/validator/v10 v10.10.1
	github.com/go-redis/cache/v8 v8.4.1
	github.com/go-redis/redis/v8 v8.10.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocarina/gocsv v0.0.0-20220310154401-d4df709ca055
	github.com/gofrs/flock v0.8.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/golang/mock v1.6.0
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/go-cmp v0.5.7
	github.com/google/go-github v17.0.0+incompatible
	github.com/google/uuid v1.2.0
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/jarcoal/httpmock v1.0.8
	github.com/looplab/fsm v0.3.0
	github.com/mcuadros/go-gin-prometheus v0.1.0
	github.com/mdlayher/vsock v1.1.1
	github.com/mitchellh/mapstructure v1.4.1
	github.com/montanaflynn/stats v0.6.6
	github.com/onsi/ginkgo/v2 v2.1.0
	github.com/onsi/gomega v1.18.0
	github.com/opencontainers/go-digest v1.0.0
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/schollz/progressbar/v3 v3.8.2
	github.com/serialx/hashring v0.0.0-20200727003509-22c0c7ab6b1b
	github.com/shirou/gopsutil/v3 v3.21.11
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.2.1
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.7.0
	github.com/swaggo/files v0.0.0-20210815190702-a29dd2bc99b2
	github.com/swaggo/gin-swagger v1.3.1
	github.com/swaggo/swag v1.8.0
	go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v0.29.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.29.0
	go.opentelemetry.io/otel v1.4.1
	go.opentelemetry.io/otel/exporters/jaeger v1.4.1
	go.opentelemetry.io/otel/sdk v1.4.1
	go.opentelemetry.io/otel/trace v1.4.1
	go.uber.org/atomic v1.9.0
	go.uber.org/zap v1.19.0
	golang.org/x/crypto v0.0.0-20220307211146-efcb8507fb70
	golang.org/x/oauth2 v0.0.0-20210819190943-2bc19b11175f
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20220307203707-22a9840ba4d7
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/api v0.44.0
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	gorm.io/driver/mysql v1.0.5
	gorm.io/gorm v1.21.15
	gorm.io/plugin/soft_delete v1.0.2
	k8s.io/apimachinery v0.23.3
	k8s.io/component-base v0.23.3
	moul.io/zapgorm2 v1.1.0
)

require (
	cloud.google.com/go v0.81.0 // indirect
	cloud.google.com/go/pubsub v1.10.0 // indirect
	github.com/Knetic/govaluate v3.0.1-0.20171022003610-9aa49832a739+incompatible // indirect
	github.com/KyleBanks/depth v1.2.1 // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/RichardKnop/logging v0.0.0-20190827224416-1a693bdd4fae // indirect
	github.com/aws/aws-sdk-go v1.37.16 // indirect
	github.com/baiyubin/aliyun-sts-go-sdk v0.0.0-20180326062324-cfa1a18b161f // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/denisenkom/go-mssqldb v0.0.0-20200428022330-06a60b6afbbc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-echarts/go-echarts/v2 v2.2.3 // indirect
	github.com/go-logr/logr v1.2.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.19.6 // indirect
	github.com/go-openapi/spec v0.20.4 // indirect
	github.com/go-openapi/swag v0.21.1 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-redsync/redsync/v4 v4.0.4 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/golang-jwt/jwt/v4 v4.0.0 // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.0 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.8.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.1.1 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/pgtype v1.6.2 // indirect
	github.com/jackc/pgx/v4 v4.10.1 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.1 // indirect
	github.com/jcmturner/rpc/v2 v2.0.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.2 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/kelseyhightower/envconfig v1.4.0 // indirect
	github.com/klauspost/compress v1.13.1 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/magiconair/properties v1.8.5 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/mdlayher/socket v0.2.0 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/opencontainers/image-spec v1.0.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pelletier/go-toml v1.9.3 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.28.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spf13/afero v1.6.0 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/streadway/amqp v1.0.0 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/ugorji/go/codec v1.2.7 // indirect
	github.com/vmihailenco/bufpool v0.1.11 // indirect
	github.com/vmihailenco/go-tinylfu v0.2.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.1.0 // indirect
	github.com/vmihailenco/tagparser v0.1.2 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.0.2 // indirect
	github.com/xdg-go/stringprep v1.0.2 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.mongodb.org/mongo-driver v1.7.2 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.opentelemetry.io/otel/internal/metric v0.27.0 // indirect
	go.opentelemetry.io/otel/metric v0.27.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/exp v0.0.0-20201221025956-e89b829e73ea // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/mod v0.5.1 // indirect
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.9 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220308174144-ae0e22291548 // indirect
	gopkg.in/ini.v1 v1.62.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gorm.io/driver/postgres v1.0.8 // indirect
	gorm.io/driver/sqlserver v1.0.4 // indirect
	gorm.io/plugin/dbresolver v1.1.0 // indirect
	k8s.io/klog/v2 v2.30.0 // indirect
)
