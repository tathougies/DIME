module TableMapper (
) where

import Data.Maybe
import qualified Data.Map as M

import Foreign

type TableMapMemoryMapper = Maybe (Int64, Int) -> IO (Ptr a, Int, Int)

data ColumnScheme = ColumnScheme {
      getRangeTree :: R.RangeTree Int64 Peer
    }

data TableScheme = TableScheme {
      getTableName :: String,
      getTableDescription :: String,
      getTableColumns :: [ColumnScheme]
    }

data TableMapper = TableMapper {
      getFilePath :: FilePath,
      getTables :: M.Map String TableScheme
      }

defaultTableMapper :: FilePath -> TableMapper
defaultTableMapper tableMapFn = tableMapper defaultTableMapMemoryMapper tableMapFn

 :: TableMapper -> 

