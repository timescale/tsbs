package devops

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

const (
	OneTerabyte = 1 << 40
	inodeSize   = 4096
)

var (
	labelDisk             = []byte("disk") // heap optimization
	TotalByteString       = []byte("total")
	FreeByteString        = []byte("free")
	UsedByteString        = []byte("used")
	UsedPercentByteString = []byte("used_percent")
	INodesTotalByteString = []byte("inodes_total")
	INodesFreeByteString  = []byte("inodes_free")
	INodesUsedByteString  = []byte("inodes_used")

	DiskTags = [][]byte{
		[]byte("path"),
		[]byte("fstype"),
	}
	DiskFSTypeChoices = [][]byte{
		[]byte("ext3"),
		[]byte("ext4"),
		[]byte("btrfs"),
	}
)

type DiskMeasurement struct {
	*subsystemMeasurement

	path, fsType []byte
	uptime       time.Duration
}

func NewDiskMeasurement(start time.Time) *DiskMeasurement {
	path := []byte(fmt.Sprintf("/dev/sda%d", rand.Intn(10)))
	fsType := DiskFSTypeChoices[rand.Intn(len(DiskFSTypeChoices))]
	sub := newSubsystemMeasurement(start, 1)
	sub.distributions[0] = common.CWD(common.ND(50, 1), 0, OneTerabyte, OneTerabyte/2)

	return &DiskMeasurement{
		subsystemMeasurement: sub,
		path:                 path,
		fsType:               fsType,
	}
}

func (m *DiskMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(labelDisk)
	p.SetTimestamp(&m.timestamp)

	p.AppendTag(DiskTags[0], m.path)
	p.AppendTag(DiskTags[1], m.fsType)

	// the only thing that actually changes is the free byte count:
	free := int64(m.distributions[0].Get())

	total := int64(OneTerabyte)
	used := total - free
	usedPercent := int64(100.0 * (float64(used) / float64(total)))

	inodesTotal := total / inodeSize
	inodesFree := free / inodeSize
	inodesUsed := used / inodeSize

	p.AppendField(TotalByteString, total)
	p.AppendField(FreeByteString, free)
	p.AppendField(UsedByteString, used)
	p.AppendField(UsedPercentByteString, usedPercent)
	p.AppendField(INodesTotalByteString, inodesTotal)
	p.AppendField(INodesFreeByteString, inodesFree)
	p.AppendField(INodesUsedByteString, inodesUsed)
}
