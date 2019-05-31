package devops

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

const (
	oneTerabyte = 1 << 40
	inodeSize   = 4096
	pathFmt     = "/dev/sda%d"
)

var (
	labelDisk            = []byte("disk") // heap optimization
	labelDiskTotal       = []byte("total")
	labelDiskFree        = []byte("free")
	labelDiskUsed        = []byte("used")
	labelDiskUsedPercent = []byte("used_percent")
	labelDiskINodesTotal = []byte("inodes_total")
	labelDiskINodesFree  = []byte("inodes_free")
	labelDiskINodesUsed  = []byte("inodes_used")

	labelDiskPath   = []byte("path")
	labelDiskFSType = []byte("fstype")

	fsExt3            = []byte("ext3")
	fsExt4            = []byte("ext4")
	fsBtrfs           = []byte("btrfs")
	diskFSTypeChoices = [][]byte{
		fsExt3,
		fsExt4,
		fsBtrfs,
	}

	diskFields = [][]byte{
		labelDiskTotal,
		labelDiskFree,
		labelDiskUsed,
		labelDiskUsedPercent,
		labelDiskINodesTotal,
		labelDiskINodesFree,
		labelDiskINodesUsed,
	}
)

type DiskMeasurement struct {
	*common.SubsystemMeasurement

	path, fsType []byte
	uptime       time.Duration
}

func NewDiskMeasurement(start time.Time) *DiskMeasurement {
	path := []byte(fmt.Sprintf(pathFmt, rand.Intn(10)))
	fsType := common.RandomByteStringSliceChoice(diskFSTypeChoices)
	sub := common.NewSubsystemMeasurement(start, 1)
	sub.Distributions[0] = common.CWD(common.ND(50, 1), 0, oneTerabyte, oneTerabyte/2)

	return &DiskMeasurement{
		SubsystemMeasurement: sub,
		path:                 path,
		fsType:               fsType,
	}
}

func (m *DiskMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(labelDisk)
	p.SetTimestamp(&m.Timestamp)

	p.AppendTag(labelDiskPath, m.path)
	p.AppendTag(labelDiskFSType, m.fsType)

	// the only thing that actually changes is the free byte count:
	free := int64(m.Distributions[0].Get())

	total := int64(oneTerabyte)
	used := total - free
	usedPercent := int64(100.0 * (float64(used) / float64(total)))

	// TODO Not sure this is strictly accurate, since integer division rounds
	// down this will give slightly inaccurate results for free & used. Ideally
	// the free distribution is clamped to the nearest inode size, but that's
	// a bigger change for another day.
	//
	// Previously, dividing all values by inodeSize could give broken semantics
	// like total != free + used, so at least that is fixed.
	inodesTotal := total / inodeSize
	inodesFree := free / inodeSize
	inodesUsed := inodesTotal - inodesFree

	p.AppendField(diskFields[0], total)
	p.AppendField(diskFields[1], free)
	p.AppendField(diskFields[2], used)
	p.AppendField(diskFields[3], usedPercent)
	p.AppendField(diskFields[4], inodesTotal)
	p.AppendField(diskFields[5], inodesFree)
	p.AppendField(diskFields[6], inodesUsed)
}
