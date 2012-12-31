module Database.DIME.DataServer.Response
    (Response(..),
     isFailure)
    where

import Database.DIME.Memory.Block (ColumnValue, ColumnType)
import Database.DIME.Memory.BlockInfo

import Data.Binary
import Data.Int

import Control.Monad

data Response = Ok |
                BlockInfoResponse BlockInfo |
                FetchRowsResponse [[ColumnValue]] |
                -- Failures
                BlockAlreadyExists |
                BlockDoesNotExist |
                InconsistentTypes |
                InconsistentArguments |
                Fail String
              deriving (Show)

okTag, fetchRowsResponseTag, blockAlreadyExistsTag, blockDoesNotExistTag, blockInfoResponseTag, failTag :: Int8
inconsistentTypesTag, inconsistentArgumentsTag :: Int8
okTag = 1
blockInfoResponseTag = 2
fetchRowsResponseTag = 3
blockAlreadyExistsTag = 4
blockDoesNotExistTag = 5
failTag = 6
inconsistentTypesTag = 7
inconsistentArgumentsTag = 8

instance Binary Response where
    put Ok = put okTag
    put (BlockInfoResponse resp) = do
                           put blockInfoResponseTag
                           put resp
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
        4 {- blockAlreadyExistsTag -} -> return BlockAlreadyExists
        5 {- blockDoesNotExistTag -} -> return BlockDoesNotExist
        6 {- failTag -} -> doFailResponse
        7 {- inconsistentTypesTag -} -> return InconsistentTypes
        8 {- inconsistentArgumentsTag -} -> return InconsistentArguments
      where
        doBlockInfoResponse = liftM BlockInfoResponse get
        doFetchRowsResponse = liftM FetchRowsResponse get
        doFailResponse = liftM Fail get

isFailure :: Response -> Bool
isFailure Ok = False
isFailure (BlockInfoResponse _) = False
isFailure (FetchRowsResponse _) = False
isFailure _ = True