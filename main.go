package main

// USAGE: ./peer_stats ./node1/ /data/bitnodes/stripped/ /data/bitnodes/timestamps.txt

import (
    "bufio"
    "fmt"
    "os"
    "strconv"
    "strings"
    "time"
)

// AgeBuckets holds count of age buckets
type AgeBuckets struct {
    LessThanOne       int
    OneToFive         int
    FiveToTen         int
    TenToThirty       int
    GreaterThanThirty int
}

// Result holds the result of computation
type Result struct {
    NumberOfReachableIPs int
    TotalIPs             int
    Percentage           float64
    OldestIPAge          uint32
    Age                  AgeBuckets
}

// CreateResult returns new object
func CreateResult() *Result {
    res := &Result{
        Age: AgeBuckets{
            LessThanOne:       0,
            OneToFive:         0,
            FiveToTen:         0,
            TenToThirty:       0,
            GreaterThanThirty: 0,
        },
    }

    return res
}

// ComputeStats computes the following stats
// 1. oldest IP in each table
// 2. Total reachable IPs in each table (at approximate age)
// 3. Percentage of reachable IPs
// 4. Agewise distribution of IPs
func ComputeStats(bitnodeFilePath string, approxAge uint32, newTableIPs, triedTableIPs []CAddrInfo) (*Result, *Result) {
    // initialize results object
    newResults := CreateResult()
    triedResults := CreateResult()

    // we first add peers.dat IPs to a map
    newSeenHashMap := make(map[string]bool)
    triedSeenHashMap := make(map[string]bool)

    for i := 0; i < len(newTableIPs); i++ {
        ip := newTableIPs[i].Address.PeerAddress.String()
        // clip off the port number
        newSeenHashMap[ip[:len(ip)-5]] = true

        AddToAgeBucket(&newResults.Age, newTableIPs[i].Address.Time, approxAge)
    }

    for i := 0; i < len(triedTableIPs); i++ {
        ip := triedTableIPs[i].Address.PeerAddress.String()
        // clip off the port number
        triedSeenHashMap[ip[:len(ip)-5]] = true

        AddToAgeBucket(&triedResults.Age, triedTableIPs[i].Address.Time, approxAge)
    }

    // now checking if these IPs exist in the bitnode db
    var newReachableIPs []string
    var triedReachableIPs []string

    // we read the bitnode file line by line and check if the address exists in our map
    bitnodeFile, _ := os.Open(bitnodeFilePath)
    scanner := bufio.NewScanner(bitnodeFile)
    defer bitnodeFile.Close()

    totalIPCount := 0
    for scanner.Scan() {
        ip := scanner.Text()
        if _, found := newSeenHashMap[ip]; found {
            newReachableIPs = append(newReachableIPs, ip)
        }
        if _, found := triedSeenHashMap[ip]; found {
            triedReachableIPs = append(triedReachableIPs, ip)
        }
        totalIPCount++
    }

    // add other stats
    newResults.NumberOfReachableIPs = len(newReachableIPs)
    newResults.TotalIPs = len(newTableIPs)
    newResults.Percentage = float64(len(newReachableIPs)) / float64(len(newTableIPs))

    triedResults.NumberOfReachableIPs = len(triedReachableIPs)
    triedResults.TotalIPs = len(triedTableIPs)
    triedResults.Percentage = float64(len(triedReachableIPs)) / float64(len(triedTableIPs))

    // finally compute oldestIP in each table
    newResults.OldestIPAge = OldestIP(newTableIPs)
    triedResults.OldestIPAge = OldestIP(triedTableIPs)

    return newResults, triedResults

}

// ClosestBitnodeTS uses binary search to find the closest bitnode timestamp
func ClosestBitnodeTS(tsFilePath string, approxAge uint32) uint32 {
    tsFile, _ := os.Open(tsFilePath)
    scanner := bufio.NewScanner(tsFile)

    defer tsFile.Close()

    var tsArray []uint32

    // load timestamps into memory
    for scanner.Scan() {
        tsInt, _ := strconv.Atoi(scanner.Text())
        tsArray = append(tsArray, uint32(tsInt))
    }

    closest := BinSearch(0, len(tsArray), approxAge, tsArray)
    return closest
}

// BinSearch modified binary search to find closest value in an array
func BinSearch(low, high int, elem uint32, tsArray []uint32) uint32 {
    mid := (high + low) / 2
    if tsArray[mid] == elem {
        // found element
        return elem
    }

    if mid == low {
        // we have reached the end, return closest element
        if elem-tsArray[low] < tsArray[high]-elem {
            return tsArray[low]
        } else {
            return tsArray[high]
        }
    }

    // continue with the search
    if elem < tsArray[mid] {
        // check left half
        newHigh := mid
        return BinSearch(low, newHigh, elem, tsArray)
    } else {
        // check right half
        newLow := mid
        return BinSearch(newLow, high, elem, tsArray)
    }
}

// OldestIP returns the timestamp of the oldest
func OldestIP(table []CAddrInfo) uint32 {
    var oldestIP uint32 = 2000000000 // suffiently large number

    for i := 0; i < len(table); i++ {
        if oldestIP > table[i].Address.Time {
            oldestIP = table[i].Address.Time
        }
    }

    return oldestIP
}

// ApproxAge guesses approx time when peers.dat was saved
func ApproxAge(peersDb PeersDB) uint32 {
    var approxAge uint32 = 0 // suffiently large number
    var i uint32
    for i = 0; i < peersDb.NNew; i++ {
        if approxAge < peersDb.NewAddrInfo[i].Address.Time {
            approxAge = peersDb.NewAddrInfo[i].Address.Time
        }
    }

    for i = 0; i < peersDb.NTried; i++ {
        if approxAge < peersDb.TriedAddrInfo[i].Address.Time {
            approxAge = peersDb.TriedAddrInfo[i].Address.Time
        }
    }

    fmt.Printf("Approx Age: %d\n", approxAge)
    return approxAge
}

// WriteArrayToFile takes array and writes to file
func WriteArrayToFile(file *os.File, array []string) {
    for i := 0; i < len(array); i++ {
        file.WriteString(array[i] + "\n")
    }
}

const ONE_DAY = 24 * 60 * 60
const FIVE_DAYS = 5 * ONE_DAY
const TEN_DAYS = 2 * FIVE_DAYS
const THIRTY_DAYS = 3 * TEN_DAYS

// AddToAgeBucket computes age in days and increments respective age bucket
func AddToAgeBucket(ageBucket *AgeBuckets, ipTimestamp, approxAge uint32) {

    ipAge := int(approxAge - ipTimestamp)

    if ipAge <= ONE_DAY {
        ageBucket.LessThanOne++
    } else if ipAge < FIVE_DAYS {
        ageBucket.OneToFive++
    } else if ipAge < TEN_DAYS {
        ageBucket.FiveToTen++
    } else if ipAge < THIRTY_DAYS {
        ageBucket.TenToThirty++
    } else {
        ageBucket.GreaterThanThirty++
    }

}

// WriteOutput dumps everything into files
func WriteOutput(approxAge uint32, newResult, triedResult *Result, basePath string) {
    newFile, _ := os.Create(basePath + "new-table-stats.txt")
    triedFile, _ := os.Create(basePath + "tried-table-stats.txt")

    defer newFile.Close()
    defer triedFile.Close()

    header := "Approx_Peerdat_Date,Oldest_IP_Days,Total_IPs,PercentReachable,Age_1,Age_1_5,Age_5_10,Age_10_30,Age_30"

    newFile.WriteString(header + "\n")
    triedFile.WriteString(header + "\n")

    pretty := func(result *Result) string {
        approxAgeT := time.Unix(int64(approxAge), 0)
        approxAgeStr := approxAgeT.Format("Jan 2 2006")

        daysOldestIP := strconv.Itoa((int(approxAge) - int(result.OldestIPAge)) / ONE_DAY)
        totalIPs := strconv.Itoa(result.TotalIPs)
        percent := strconv.FormatFloat(result.Percentage*100, 'f', 2, 64)

        age_1 := strconv.Itoa(result.Age.LessThanOne)
        age_1_5 := strconv.Itoa(result.Age.OneToFive)
        age_5_10 := strconv.Itoa(result.Age.FiveToTen)
        age_10_30 := strconv.Itoa(result.Age.TenToThirty)
        age_30 := strconv.Itoa(result.Age.GreaterThanThirty)

        resultSlice := []string{approxAgeStr, daysOldestIP, totalIPs, percent, age_1, age_1_5, age_5_10, age_10_30, age_30}
        return strings.Join(resultSlice, ",")
    }

    newFile.WriteString(pretty(newResult) + "\n")
    triedFile.WriteString(pretty(triedResult) + "\n")
}

func main() {
    // get base path from first argument
    basePath := os.Args[1]
    // get bitnode timestamp directory from second
    bitnodeBasePath := os.Args[2]
    // get timestamps.txt path from third
    tsFilePath := os.Args[3]

    peersFilePath := basePath + "peers.dat"

    rawPeersDB, err := NewPeersDB(peersFilePath)

    if err != nil {
        fmt.Println(err)
    }

    peersDb := PeersDB(rawPeersDB)

    // get approx time when the file was saved
    approxAge := ApproxAge(peersDb)

    // get closest bitnode timestamp
    bitnodeTS := ClosestBitnodeTS(tsFilePath, approxAge)
    fmt.Printf("Closest bitnode timestamp: %d\n", bitnodeTS)

    // get the set of reachable IPs
    bitnodeBasePath += strconv.Itoa(int(bitnodeTS)) + ".txt"
    newResult, oldResult := ComputeStats(bitnodeBasePath, approxAge, peersDb.NewAddrInfo, peersDb.TriedAddrInfo)

    // write output
    WriteOutput(approxAge, newResult, oldResult, basePath)
}
