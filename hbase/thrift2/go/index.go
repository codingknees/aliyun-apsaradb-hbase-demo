package main
import (
    // hbase模块通过 thrift --gen go hbase.thrift 来生成
    "demo/gen-go/hbase"
    "context"
    "fmt"
    "github.com/apache/thrift/lib/go/thrift"
    "os"
    "time"
)

func query(client *hbase.THBaseServiceClient) {
    defaultCtx := context.Background()
    tableInbytes := []byte("dw:dwd_tt_ad_click")
    maxVersions := int32(3000)
    // 单行查询数据
    _, err := client.Get(defaultCtx, tableInbytes, &hbase.TGet{Row: []byte("0.0.711.322"), Columns: []*hbase.TColumn{{Family: []byte("cf"), Qualifier: []byte("v")}}, MaxVersions: &maxVersions})
    if err != nil {
        fmt.Println("err: ", *(err.(*hbase.TIOError).Message))
        return
    }
    // fmt.Println("Get result:")
    // fmt.Println(result)
}
func scan(client *hbase.THBaseServiceClient) {
    defaultCtx := context.Background()
    tableInbytes := []byte("dw:dwd_tt_ad_click")
    maxVersions := int32(3000)

    //范围扫描
    //扫描时需要设置startRow和stopRow，否则会变成全表扫描
    startRow := []byte("0.0.711.322")
    stopRow := []byte("0.0.72.111")
    scan := &hbase.TScan{StartRow: startRow, StopRow: stopRow, Columns: []*hbase.TColumn{{Family: []byte("cf"), Qualifier: []byte("v")}}, MaxVersions: maxVersions}
    //caching的大小为每次从服务器返回的行数，设置太大会导致服务器处理过久，太小会导致范围扫描与服务器做过多交互
    //根据每行的大小，caching的值一般设置为10到100之间
    caching := 2
    // 扫描的结果
    var scanResults []*hbase.TResult_
    for true {
        var lastResult *hbase.TResult_ = nil
        // getScannerResults会自动完成open,close 等scanner操作，HBase增强版必须使用此方法进行范围扫描
        currentResults, err := client.GetScannerResults(defaultCtx, tableInbytes, scan, int32(caching))
        if err != nil {
            fmt.Println("err:", err)
            return
        }
        for _, tResult := range currentResults {
            lastResult = tResult
            scanResults = append(scanResults, tResult)
        }
        // 如果一行都没有扫描出来，说明扫描已经结束，我们已经获得startRow和stopRow之间所有的result
        if lastResult == nil {
            break
        } else {
            // 如果此次扫描是有结果的，我们必须构造一个比当前最后一个result的行大的最小row，继续进行扫描，以便返回所有结果
            nextStartRow := createClosestRowAfter(lastResult.Row)
            scan = &hbase.TScan{StartRow: nextStartRow, StopRow: stopRow}
        }
    }
    // fmt.Println("Scan result:")
    // fmt.Println(scanResults)
}
func main() {
    HOST := os.Getenv("HOST")
    USER := os.Getenv("USER")
    PASSWD := os.Getenv("PASSWD")

    protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
    trans, err := thrift.NewTHttpClient(HOST)
     // 设置用户名密码
    httClient := trans.(*thrift.THttpClient)
    httClient.SetHeader("ACCESSKEYID", USER)
    httClient.SetHeader("ACCESSSIGNATURE", PASSWD)
    if err != nil {
        fmt.Fprintln(os.Stderr, "error resolving address:", err)
        os.Exit(1)
    }
    client := hbase.NewTHBaseServiceClientFactory(trans, protocolFactory)
    if err := trans.Open(); err != nil {
        fmt.Fprintln(os.Stderr, "Error opening "+HOST, err)
        os.Exit(1)
    }
    
    for i := 0; i < 1; i ++ {
        now := time.Now()
        query(client)
        fmt.Println("time costs: ", time.Since(now).Milliseconds())
    }
    
    return
    for i := 0; i < 1000; i ++ {
        now := time.Now()
        scan(client)
        fmt.Println("time cost: ", time.Since(now).Milliseconds())
    }
}

func createClosestRowAfter(row []byte) []byte {
    var nextRow []byte
    var i int
    for i = 0; i < len(row); i++ {
        nextRow = append(nextRow, row[i])
    }
    nextRow = append(nextRow, 0x00)
    return nextRow
 }
