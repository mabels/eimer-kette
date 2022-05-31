package frontend

// import (
// 	"context"
// 	"crypto/sha256"
// 	"encoding/json"
// 	"fmt"
// 	"math/rand"
// 	"os"
// 	"strings"
// 	"testing"
// 	"time"

// 	"github.com/aws/aws-sdk-go-v2/service/s3"
// 	"github.com/aws/aws-sdk-go-v2/service/s3/types"
// 	"github.com/aws/aws-sdk-go/aws"
// 	"github.com/hashicorp/go-memdb"
// 	"github.com/reactivex/rxgo/v2"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/suite"
// )

// type FastCreate struct {
// 	topLevel string
// 	year     int
// 	month    int
// 	day      int
// 	idx      int
// }

// func CreateTestData() *memdb.MemDB {
// 	schema := &memdb.DBSchema{
// 		Tables: map[string]*memdb.TableSchema{
// 			"files": {
// 				Name: "files",
// 				Indexes: map[string]*memdb.IndexSchema{
// 					"id": {
// 						Name:    "id",
// 						Unique:  true,
// 						Indexer: &memdb.StringFieldIndex{Field: "Key"},
// 					},
// 				},
// 			},
// 		},
// 	}

// 	// Create a new data base
// 	db, err := memdb.NewMemDB(schema)
// 	if err != nil {
// 		panic(err)
// 	}
// 	r := rand.New(rand.NewSource(4711))

// 	obs := rxgo.Defer([]rxgo.Producer{func(_ context.Context, ch chan<- rxgo.Item) {
// 		for _, topLevel := range []string{"A-Ast1", "A-Bast1", "A-Knast2", "A-Last5"} {
// 			for _, year := range []int{2010, 2020} {
// 				for _, month := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12} {
// 					for _, day := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30} {
// 						for idx := 200 + r.Intn(3000); idx > 200; idx-- {
// 							ch <- rxgo.Item{V: FastCreate{
// 								topLevel: topLevel,
// 								year:     year,
// 								month:    month,
// 								day:      day,
// 								idx:      idx,
// 							}}
// 						}
// 					}
// 				}
// 			}
// 		}
// 	}}).BufferWithCount(1000).Map(func(_ context.Context, items interface{}) (interface{}, error) {
// 		layout := "2006-01-02T15:04:05.000Z"
// 		myId := "myId"
// 		myDn := "myDn"
// 		txn := db.Txn(true)
// 		for _, istr := range items.([]interface{}) {
// 			fc := istr.(FastCreate)
// 			base := fmt.Sprintf("%s/year=%d/month=%d/day=%d", fc.topLevel, fc.year, fc.month, fc.day)
// 			fname := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%d", base, fc.idx))))
// 			key := fmt.Sprintf("%s/%s.json", base, fname)
// 			t, _ := time.Parse(layout, fmt.Sprintf("%d-%02d-%02dT%02d:%02d:%02d.%03dZ",
// 				fc.year, fc.month, fc.day, 11, 27, (fc.idx/1000)%60, fc.idx%1000))
// 			txn.Insert("files", types.Object{
// 				ETag:         &fname,
// 				Key:          &key,
// 				LastModified: &t,
// 				Owner: &types.Owner{
// 					DisplayName: &myDn,
// 					ID:          &myId,
// 				},
// 				Size:         int64(fc.idx),
// 				StorageClass: types.ObjectStorageClassStandard,
// 			})
// 		}
// 		txn.Commit()
// 		return nil, nil
// 	}, rxgo.WithPool(8))

// 	for range obs.Observe() {

// 	}
// 	return db
// }

// type S3Mock struct {
// 	db       *memdb.MemDB
// 	pageSize int
// 	random   *rand.Rand
// }

// // seed := 99

// func MakeS3Mock() S3Mock {
// 	return S3Mock{
// 		random:   rand.New(rand.NewSource(time.Now().UnixNano())),
// 		db:       CreateTestData(),
// 		pageSize: 1000,
// 	}
// }

// type S3Cursor struct {
// 	txn *memdb.Txn
// 	it  memdb.ResultIterator
// }

// type S3Client struct {
// 	s3     *S3Mock
// 	cursor map[string]*S3Cursor
// 	awsCfg aws.Config
// }

// func (sm *S3Mock) NewFromConfig(cfg aws.Config) *S3Client {
// 	return &S3Client{
// 		s3:     sm,
// 		awsCfg: cfg,
// 		cursor: map[string]*S3Cursor{},
// 	}
// }

// func (s3c *S3Client) ListObjectsV2(_ context.Context, inp *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
// 	if inp.Bucket == nil || len(*inp.Bucket) == 0 {
// 		return nil, fmt.Errorf("You need a bucket name")
// 	}
// 	var cursor *S3Cursor
// 	if inp.ContinuationToken == nil {
// 		token := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%d", s3c.s3.random.Uint64()))))
// 		inp.ContinuationToken = &token
// 		cursor = &S3Cursor{}
// 		s3c.cursor[*inp.ContinuationToken] = cursor
// 		cursor.txn = s3c.s3.db.Txn(false)
// 	} else {
// 		var ok bool
// 		cursor, ok = s3c.cursor[*inp.ContinuationToken]
// 		if !ok {
// 			return nil, fmt.Errorf("unknown cursor")
// 		}
// 	}
// 	fmt.Fprintln(os.Stderr, "Prefix=", *inp.Prefix)
// 	var err error
// 	cursor.it, err = cursor.txn.LowerBound("files", "id", *inp.Prefix)
// 	if err != nil {
// 		return nil, err
// 	}
// 	outObject := make([]types.Object, 0, s3c.s3.pageSize)
// 	outCommonPrefixMap := map[string]bool{}
// 	//make([]types.CommonPrefix, 0, s3c.s3.pageSize)

// 	cnt := 0
// 	var obj interface{}
// 	if inp.Delimiter == nil {
// 		empty := ""
// 		inp.Delimiter = &empty
// 	}
// 	for obj = cursor.it.Next(); obj != nil; obj = cursor.it.Next() {
// 		p := obj.(types.Object)
// 		// fmt.Fprintln(os.Stderr, "cursor=", *p.Key)
// 		idx := strings.Index((*p.Key)[len(*inp.Prefix):], *inp.Delimiter)
// 		if idx < 0 {
// 			cnt++
// 			outObject = append(outObject, p)
// 		} else {
// 			prefix := (*p.Key)[0 : len(*inp.Prefix)+idx]
// 			_, ok := outCommonPrefixMap[prefix]
// 			if !ok {
// 				fmt.Fprintf(os.Stderr, "commonPrefix=%s-%s\n", prefix, *inp.Delimiter)
// 				cnt++
// 				outCommonPrefixMap[prefix] = true
// 			}
// 		}
// 		if s3c.s3.pageSize <= cnt {
// 			break
// 		}
// 	}
// 	if obj == nil {
// 		inp.ContinuationToken = nil
// 	}
// 	outCommonPrefix := make([]types.CommonPrefix, 0, len(outCommonPrefixMap))
// 	for cp := range outCommonPrefixMap {
// 		fmt.Fprintf(os.Stderr, "co=%s\n", cp)
// 		out := cp
// 		outCommonPrefix = append(outCommonPrefix, types.CommonPrefix{Prefix: &out})
// 	}
// 	startAfter := "startAfter"
// 	return &s3.ListObjectsV2Output{
// 		CommonPrefixes:    outCommonPrefix,
// 		Contents:          outObject,
// 		ContinuationToken: inp.ContinuationToken,

// 		Delimiter: inp.Delimiter,

// 		EncodingType: inp.EncodingType,

// 		IsTruncated: s3c.s3.pageSize <= cnt,

// 		KeyCount: int32(cnt),

// 		MaxKeys: int32(s3c.s3.pageSize),

// 		Name: inp.Bucket,

// 		NextContinuationToken: inp.ContinuationToken,

// 		Prefix:     inp.Prefix,
// 		StartAfter: &startAfter,

// 		// ResultMetadata middleware.Metadata

// 	}, nil
// }

// type FrontendSuite struct {
// 	suite.Suite
// 	s3 S3Mock
// 	// mockedSvalFn *SvalFnMock
// }

// func (s *FrontendSuite) SetupTest() {
// 	// s.T().Error("---1")
// 	fmt.Fprintln(os.Stderr, "---1")
// 	s.s3 = MakeS3Mock()
// 	fmt.Fprintln(os.Stderr, "---2")
// 	// s.mockedSvalFn = &SvalFnMock{}
// }

// func (s *FrontendSuite) TestStart() {
// 	client := s.s3.NewFromConfig(aws.Config{})
// 	bucket := "Test"
// 	prefix := ""
// 	delimiter := "/"
// 	resp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
// 		Bucket:    &bucket,
// 		Prefix:    &prefix,
// 		Delimiter: &delimiter,
// 	})
// 	assert.NoError(s.T(), err)
// 	jsonDb, _ := json.Marshal(resp)
// 	assert.Equal(s.T(), string(jsonDb), "{\"CommonPrefixes\":[{\"Prefix\":\"A-Ast1\"},{\"Prefix\":\"A-Bast1\"},{\"Prefix\":\"A-Knast2\"},{\"Prefix\":\"A-Last5\"}],\"Contents\":[],\"ContinuationToken\":null,\"Delimiter\":\"/\",\"EncodingType\":\"\",\"IsTruncated\":false,\"KeyCount\":4,\"MaxKeys\":1000,\"Name\":\"Test\",\"NextContinuationToken\":null,\"Prefix\":\"\",\"StartAfter\":\"startAfter\",\"ResultMetadata\":{}}")
// }

// func (s *FrontendSuite) TestFix() {
// 	client := s.s3.NewFromConfig(aws.Config{})
// 	bucket := "Test"
// 	prefix := ""
// 	delimiter := ""
// 	resp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
// 		Bucket:    &bucket,
// 		Prefix:    &prefix,
// 		Delimiter: &delimiter,
// 	})
// 	assert.NoError(s.T(), err)
// 	jsonDb, _ := json.Marshal(resp)
// 	assert.Equal(s.T(), string(jsonDb), "{\"CommonPrefixes\":[{\"Prefix\":\"A-Ast1\"},{\"Prefix\":\"A-Bast1\"},{\"Prefix\":\"A-Knast2\"},{\"Prefix\":\"A-Last5\"}],\"Contents\":[],\"ContinuationToken\":null,\"Delimiter\":\"/\",\"EncodingType\":\"\",\"IsTruncated\":false,\"KeyCount\":4,\"MaxKeys\":1000,\"Name\":\"Test\",\"NextContinuationToken\":null,\"Prefix\":\"\",\"StartAfter\":\"startAfter\",\"ResultMetadata\":{}}")
// }

// func TestFrontend(t *testing.T) {
// 	suite.Run(t, new(FrontendSuite))
// }
