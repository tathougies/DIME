{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Database.DIME
    (
     BlockID(..), TableID(..), ColumnID(..),
     RowID(..), BlockRowID(..), BlockSpec(..)
    ) where

import Data.Int

import Data.Binary
import Control.Monad
import Control.DeepSeq

import System.Time

import Text.JSON

newtype BlockID = BlockID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral, JSON, NFData, Binary, Bounded)
newtype TableID = TableID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral, JSON, NFData, Binary, Bounded)
newtype ColumnID = ColumnID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral, JSON, NFData, Binary, Bounded)
newtype RowID = RowID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral, JSON, NFData, Binary, Bounded)
newtype BlockRowID = BlockRowID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral, JSON, NFData, Binary, Bounded)

data BlockSpec = BlockSpec TableID ColumnID BlockID
    deriving (Ord, Eq, Show, Read)

instance Binary BlockSpec where
    put (BlockSpec tId cId bId) = do
      put tId
      put cId
      put bId
    get = do
      tId <- get
      cId <- get
      bId <- get
      return $ BlockSpec tId cId bId

instance Binary ClockTime where
    put (TOD seconds picoseconds) = put seconds >> put picoseconds
    get = liftM2 TOD get get

instance JSON BlockSpec where
    showJSON (BlockSpec tableId columnId blockId) = showJSON $ (tableId, columnId, blockId)
    readJSON jsValue = case (readJSON jsValue :: Result (TableID, ColumnID, BlockID)) of
                         Ok (tableId, columnId, blockId) -> Ok $ BlockSpec tableId columnId blockId
                         Error e -> Error e

instance NFData BlockSpec where
    rnf (BlockSpec tId cId bId) = rnf tId `seq` rnf cId `seq` rnf bId `seq` ()