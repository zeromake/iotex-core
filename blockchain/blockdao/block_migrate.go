package blockdao

import (
	"github.com/golang/protobuf/proto"
	"github.com/iotexproject/go-pkgs/hash"
	"github.com/iotexproject/iotex-proto/golang/iotextypes"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/iotexproject/iotex-core/blockchain/block"
	"github.com/iotexproject/iotex-core/db"
	"github.com/iotexproject/iotex-core/pkg/compress"
	"github.com/iotexproject/iotex-core/pkg/enc"
	"github.com/iotexproject/iotex-core/pkg/log"
	"github.com/iotexproject/iotex-core/pkg/util/byteutil"
)

func (dao *blockDAO) initMigration() error {
	var err error
	if dao.blockData, err = db.NewCountingIndexNX(dao.kvstore, []byte(blockDataNS)); err != nil {
		return err
	}
	if dao.receipt, err = db.NewCountingIndexNX(dao.kvstore, []byte(receiptsNS)); err != nil {
		return err
	}
	dao.batch = db.NewBatch()
	return nil
}

// GetBlockByHeightLegacy gets block in legacy way
func (dao *blockDAO) GetBlockByHeightLegacy(height uint64) (*block.Block, error) {
	h, err := dao.getBlockHash(height)
	if err != nil {
		return nil, err
	}
	return dao.getBlockLegacy(h)
}

func (dao *blockDAO) CommitForMigration() error {
	if err := dao.blockData.Commit(); err != nil {
		return err
	}
	if err := dao.receipt.Commit(); err != nil {
		return err
	}
	return dao.kvstore.Commit(dao.batch)
}

// PutBlockForMigration puts a block for migration
func (dao *blockDAO) PutBlockForMigration(blk *block.Block) error {
	blkHeight := blk.Height()
	h, err := dao.getBlockHash(blkHeight)
	if h != hash.ZeroHash256 && err == nil {
		return errors.Errorf("block %d already exist", blkHeight)
	}
	serBlk, err := blk.Serialize()
	if err != nil {
		return errors.Wrap(err, "failed to serialize block")
	}
	if dao.compressBlock {
		timer := dao.timerFactory.NewTimer("compress_header")
		serBlk, err = compress.Compress(serBlk)
		timer.End()
		if err != nil {
			return errors.Wrapf(err, "error when compressing a block")
		}
	}
	if err = dao.blockData.Add(serBlk, true); err != nil {
		return err
	}
	// write receipts
	if blk.Receipts != nil {
		receipts := iotextypes.Receipts{}
		for _, r := range blk.Receipts {
			receipts.Receipts = append(receipts.Receipts, r.ConvertToReceiptPb())
		}
		if receiptsBytes, err := proto.Marshal(&receipts); err == nil {
			if err = dao.receipt.Add(receiptsBytes, true); err != nil {
				return err
			}
		} else {
			log.L().Error("failed to serialize receipits for block", zap.Uint64("height", blkHeight))
		}
	}

	hash := blk.HashBlock()
	batch := dao.batch
	heightValue := byteutil.Uint64ToBytes(blkHeight)
	hashKey := hashKey(hash)
	batch.Put(blockHashHeightMappingNS, hashKey, heightValue, "failed to put hash -> height mapping")
	heightKey := heightKey(blkHeight)
	batch.Put(blockHashHeightMappingNS, heightKey, hash[:], "failed to put height -> hash mapping")
	tipHeight, err := dao.kvstore.Get(blockNS, topHeightKey)
	if err != nil {
		return errors.Wrap(err, "failed to get top height")
	}
	if blkHeight > enc.MachineEndian.Uint64(tipHeight) {
		batch.Put(blockNS, topHeightKey, heightValue, "failed to put top height")
		batch.Put(blockNS, topHashKey, hash[:], "failed to put top hash")
	}
	return nil
}

// getBlock returns a block
func (dao *blockDAO) getBlockLegacy(hash hash.Hash256) (*block.Block, error) {
	header, err := dao.headerLegacy(hash)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get block header %x", hash)
	}
	body, err := dao.bodyLegacy(hash)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get block body %x", hash)
	}
	footer, err := dao.footerLegacy(hash)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get block footer %x", hash)
	}
	return &block.Block{
		Header: *header,
		Body:   *body,
		Footer: *footer,
	}, nil
}

func (dao *blockDAO) headerLegacy(h hash.Hash256) (*block.Header, error) {
	value, err := dao.getBlockValue(blockHeaderNS, h)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get block header %x", h)
	}
	if dao.compressBlock {
		timer := dao.timerFactory.NewTimer("decompress_header")
		value, err = compress.Decompress(value)
		timer.End()
		if err != nil {
			return nil, errors.Wrapf(err, "error when decompressing a block header %x", h)
		}
	}
	if len(value) == 0 {
		return nil, errors.Wrapf(db.ErrNotExist, "block header %x is missing", h)
	}
	header := &block.Header{}
	if err := header.Deserialize(value); err != nil {
		return nil, errors.Wrapf(err, "failed to deserialize block header %x", h)
	}
	return header, nil
}

func (dao *blockDAO) bodyLegacy(h hash.Hash256) (*block.Body, error) {
	value, err := dao.getBlockValue(blockBodyNS, h)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get block body %x", h)
	}
	if dao.compressBlock {
		timer := dao.timerFactory.NewTimer("decompress_body")
		value, err = compress.Decompress(value)
		timer.End()
		if err != nil {
			return nil, errors.Wrapf(err, "error when decompressing a block body %x", h)
		}
	}
	if len(value) == 0 {
		return nil, errors.Wrapf(db.ErrNotExist, "block body %x is missing", h)
	}
	body := &block.Body{}
	if err := body.Deserialize(value); err != nil {
		return nil, errors.Wrapf(err, "failed to deserialize block body %x", h)
	}
	return body, nil
}

func (dao *blockDAO) footerLegacy(h hash.Hash256) (*block.Footer, error) {
	value, err := dao.getBlockValue(blockFooterNS, h)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get block footer %x", h)
	}
	if dao.compressBlock {
		timer := dao.timerFactory.NewTimer("decompress_footer")
		value, err = compress.Decompress(value)
		timer.End()
		if err != nil {
			return nil, errors.Wrapf(err, "error when decompressing a block footer %x", h)
		}
	}
	if len(value) == 0 {
		return nil, errors.Wrapf(db.ErrNotExist, "block footer %x is missing", h)
	}
	footer := &block.Footer{}
	if err := footer.Deserialize(value); err != nil {
		return nil, errors.Wrapf(err, "failed to deserialize block footer %x", h)
	}
	return footer, nil
}

// getDBFromHash returns db of this block stored
func (dao *blockDAO) getDBFromHash(h hash.Hash256) (db.KVStore, uint64, error) {
	height, err := dao.getBlockHeight(h)
	if err != nil {
		return nil, 0, err
	}
	return dao.getDBFromHeight(height)
}

// getBlockValue get block's data from db,if this db failed,it will try the previous one
func (dao *blockDAO) getBlockValue(blockNS string, h hash.Hash256) ([]byte, error) {
	whichDB, index, err := dao.getDBFromHash(h)
	if err != nil {
		return nil, err
	}
	value, err := whichDB.Get(blockNS, h[:])
	if errors.Cause(err) == db.ErrNotExist {
		idx := index - 1
		if index == 0 {
			idx = 0
		}
		db, _, err := dao.getDBFromIndex(idx)
		if err != nil {
			return nil, err
		}
		value, err = db.Get(blockNS, h[:])
		if err != nil {
			return nil, err
		}
	}
	return value, err
}
