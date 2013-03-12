module Database.DIME.Memory.BlockInfo
    (BlockInfo(..), empty)
    where

import Database.DIME.Memory.Block (ColumnType)
import Database.DIME

import Data.Binary

data BlockInfo = BlockInfo { firstRow :: BlockRowID,
                             lastRow :: BlockRowID,
                             blockType :: ColumnType
                           }
               deriving (Show, Eq)

empty = BlockInfo (BlockRowID 0) (BlockRowID 0) (error "Unspecified column type")

instance Binary BlockInfo where
    put bi = do
      put $ firstRow bi
      put $ lastRow bi
      put $ blockType bi
    get = do
      firstRow <- get
      lastRow <- get
      blockType <- get
      return $ BlockInfo firstRow lastRow blockType