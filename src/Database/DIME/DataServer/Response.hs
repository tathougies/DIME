module Database.DIME.DataServer.Response
    (Response(..),
     QueryResult(..),
     isFailure)
    where

import Database.DIME.Memory.Block (ColumnValue, ColumnType)
import Database.DIME.Memory.BlockInfo

import Language.Flow.Execution.Types

import qualified Data.Text as T
import Data.Binary
import Data.Int

import Control.Monad

import System.Time

data QueryResult = DoubleResult {-# UNPACK #-} !Double |
                   IntResult {-# UNPACK #-} !Int64 |
                   StringResult !T.Text |
                   TimeSeriesResult [(ClockTime, ColumnValue)]
              deriving (Show)

data Response = Ok |
                BlockInfoResponse BlockInfo |
                FetchRowsResponse [[ColumnValue]] |
                QueryResponse QueryResult |
                MapResponse ColumnType |
                -- Failures
                BlockAlreadyExists |
                BlockDoesNotExist |
                InconsistentTypes |
                InconsistentArguments |
                Fail String
              deriving (Show)

okTag, fetchRowsResponseTag, blockAlreadyExistsTag, blockDoesNotExistTag, blockInfoResponseTag, failTag :: Int8
inconsistentTypesTag, inconsistentArgumentsTag, queryResponseTag, mapResponseTag :: Int8
okTag = 1
blockInfoResponseTag = 2
fetchRowsResponseTag = 3
queryResponseTag = 4
mapResponseTag = 5
blockAlreadyExistsTag = 6
blockDoesNotExistTag = 7
failTag = 8
inconsistentTypesTag = 9
inconsistentArgumentsTag = 10

instance Binary Response where
    put Ok = put okTag
    put (BlockInfoResponse resp) = do
                           put blockInfoResponseTag
                           put resp
    put (QueryResponse dat) = do
                           put queryResponseTag
                           put dat
    put (MapResponse dataType) = do
                           put mapResponseTag
                           put dataType
    put BlockAlreadyExists = put blockAlreadyExistsTag
    put BlockDoesNotExist = put blockDoesNotExistTag
    put InconsistentTypes = put inconsistentTypesTag
    put InconsistentArguments = put inconsistentArgumentsTag
    put (FetchRowsResponse values) = do
                           put fetchRowsResponseTag
                           put values
    put (Fail error) = do
      put failTag
      put error

    get = do
      tag <- (get :: Get Int8)
      case tag of
        1 {- okTag -} -> return Ok
        2 {- blockInfoResponseTag -} -> doBlockInfoResponse
        3 {- fetchRowsResponseTag -} -> doFetchRowsResponse
        4 {- queryResponseTag -} -> doFetchQueryResponse
        5 {- mapResponseTag -} -> doMapResponse
        6 {- blockAlreadyExistsTag -} -> return BlockAlreadyExists
        7 {- blockDoesNotExistTag -} -> return BlockDoesNotExist
        8 {- failTag -} -> doFailResponse
        9 {- inconsistentTypesTag -} -> return InconsistentTypes
        10 {- inconsistentArgumentsTag -} -> return InconsistentArguments
      where
        doBlockInfoResponse = liftM BlockInfoResponse get
        doFetchRowsResponse = liftM FetchRowsResponse get
        doFailResponse = liftM Fail get
        doFetchQueryResponse = liftM QueryResponse get
        doMapResponse = liftM MapResponse get

doubleResultTag, intResultTag, stringResultTag, timeSeriesResultTag :: Int8
doubleResultTag = 1
intResultTag = 2
stringResultTag = 3
timeSeriesResultTag = 4

instance Binary QueryResult where
    put (DoubleResult d) = do
      put doubleResultTag
      put d
    put (IntResult i) = do
      put intResultTag
      put i
    put (StringResult t) = do
      put stringResultTag
      put t
    put (TimeSeriesResult tsData) = do
      put timeSeriesResultTag
      put tsData

    get = do
      tag <- (get :: Get Int8)
      case tag of
        1 {- doubleResultTag -} -> liftM DoubleResult get
        2 {- intResultTag -} -> liftM IntResult get
        3 {- stringResultTag -} -> liftM StringResult get
        4 {- timeSeriesResultTag -} -> liftM TimeSeriesResult get

isFailure :: Response -> Bool
isFailure Ok = False
isFailure (BlockInfoResponse _) = False
isFailure (FetchRowsResponse _) = False
isFailure (QueryResponse _) = False
isFailure (MapResponse _) = False
isFailure _ = True