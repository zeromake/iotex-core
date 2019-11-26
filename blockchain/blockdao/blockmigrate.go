// Copyright (c) 2019 IoTeX Foundation
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package blockdao

import (
	"context"
	"os"
	"path"

	"github.com/iotexproject/iotex-core/pkg/log"

	"github.com/iotexproject/go-pkgs/hash"
	"github.com/iotexproject/iotex-election/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/iotexproject/iotex-core/blockchain/block"
	"github.com/iotexproject/iotex-core/db"
	"github.com/iotexproject/iotex-core/pkg/compress"
)

const (
	blockHeaderNS = "bhr"
	blockBodyNS   = "bbd"
	blockFooterNS = "bfr"
)

var (
	tipHeightKey = []byte("th")
)

var (
	pattern = "-00000000.db"
)

func (dao *blockDAO) checkOldDB() error {
	fileExists := func(path string) bool {
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			return false
		}
		if err != nil {
			zap.L().Panic("unexpected error", zap.Error(err))
		}
		return true
	}
	ext := path.Ext(dao.cfg.DbPath)
	var fileName string
	if len(ext) > 0 {
		fileName = dao.cfg.DbPath[:len(dao.cfg.DbPath)-len(ext)] + pattern
	}
	log.L().Info("checkOldDB::", zap.String("fileName", fileName))
	if fileExists(fileName) {
		return nil
	}

	bakDbPath := path.Dir(dao.cfg.DbPath) + "/oldchain.db"
	log.L().Info("bakDbPath::", zap.String("bakDbPath:", bakDbPath))
	if err := os.Rename(dao.cfg.DbPath, bakDbPath); err != nil {
		return err
	}
	cfgDB := dao.cfg
	cfgDB.DbPath = bakDbPath
	bakdb := db.NewBoltDB(cfgDB)
	dao.oldDB = bakdb
	return nil
}

func (dao *blockDAO) migrate() error {
	if err := dao.oldDB.Start(context.Background()); err != nil {
		return err
	}
	defer dao.oldDB.Stop(context.Background())

	tipHeightValue, err := dao.oldDB.Get(blockNS, tipHeightKey)
	if err != nil {
		return err
	}
	tipHeight := util.BytesToUint64(tipHeightValue)
	log.L().Info("tipHeight:", zap.Uint64("height", tipHeight))
	for i := uint64(1); i <= tipHeight; i++ {
		h, err := dao.getOldBlockHash(i)
		if err != nil {
			return err
		}
		blk, err := dao.getOldBlock(h)
		if err != nil {
			return err
		}
		err = dao.putBlock(blk)
		if err != nil {
			return err
		}
		if i%100 == 0 {
			log.L().Info("putBlock:", zap.Uint64("height", i))
		}
	}
	return nil
}

// getBlockHash returns the block hash by height
func (dao *blockDAO) getOldBlockHash(height uint64) (hash.Hash256, error) {
	h := hash.ZeroHash256
	if height == 0 {
		return h, nil
	}
	key := heightKey(height)
	value, err := dao.oldDB.Get(blockHashHeightMappingNS, key)
	if err != nil {
		return h, errors.Wrap(err, "failed to get block hash")
	}
	if len(h) != len(value) {
		return h, errors.Wrapf(err, "blockhash is broken with length = %d", len(value))
	}
	copy(h[:], value)
	return h, nil
}

func (dao *blockDAO) getOldBlock(hash hash.Hash256) (*block.Block, error) {
	header, err := dao.oldHeader(hash)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get block header %x", hash)
	}
	body, err := dao.oldBody(hash)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get block body %x", hash)
	}
	footer, err := dao.oldFooter(hash)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get block footer %x", hash)
	}
	return &block.Block{
		Header: *header,
		Body:   *body,
		Footer: *footer,
	}, nil
}

func (dao *blockDAO) oldHeader(h hash.Hash256) (*block.Header, error) {
	value, err := dao.oldDB.Get(blockHeaderNS, h[:])
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

func (dao *blockDAO) oldBody(h hash.Hash256) (*block.Body, error) {
	value, err := dao.oldDB.Get(blockBodyNS, h[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get block header %x", h)
	}
	if dao.compressBlock {
		value, err = compress.Decompress(value)
		if err != nil {
			return nil, errors.Wrapf(err, "error when decompressing a block header %x", h)
		}
	}
	if len(value) == 0 {
		return nil, errors.Wrapf(db.ErrNotExist, "block header %x is missing", h)
	}
	body := &block.Body{}
	if err := body.Deserialize(value); err != nil {
		return nil, errors.Wrapf(err, "failed to deserialize block header %x", h)
	}
	return body, nil
}

func (dao *blockDAO) oldFooter(h hash.Hash256) (*block.Footer, error) {
	value, err := dao.oldDB.Get(blockFooterNS, h[:])
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
	footer := &block.Footer{}
	if err := footer.Deserialize(value); err != nil {
		return nil, errors.Wrapf(err, "failed to deserialize block header %x", h)
	}
	return footer, nil
}
