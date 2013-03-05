{-# LANGUAGE DeriveDataTypeable #-}
module Database.DIME.Flow.Types where

    import Control.Monad.State

    import qualified Data.Tree.DisjointIntervalTree as DIT
    import qualified Data.Map as M
    import Data.Typeable
    import Data.Dynamic
    import Data.Int

    import Database.DIME
    import Database.DIME.Memory.Block (ColumnType(..))
    import Database.DIME.DataServer.Command
    import Database.DIME.Server.State

    import Language.Flow.Execution.Types
    import Language.Flow.Execution.GMachine

    import qualified System.ZMQ3 as ZMQ
    import System.Time

    data BlockInfo = BlockInfo [PeerName]
                     deriving (Show, Eq)

    data TimeSeries = TimeSeries {
      tsTableName :: TimeSeriesName,
      tsColumnName :: ColumnName,
      tsTableId :: TableID,
      tsColumnId :: ColumnID,
      tsDataType :: ColumnType,
      tsLength :: TimeSeriesIx,
      tsStartTime :: ClockTime,
      tsFrequency :: Int64,
      tsBlocks :: M.Map BlockID BlockInfo,
      tsRowMappings :: DIT.DisjointIntervalTree RowID BlockID}
     deriving Show

    data TimeSeriesCollection = TimeSeriesCollection {
                                    tscName :: TimeSeriesName,
                                    tscTableId :: TableID,
                                    tscLength :: TimeSeriesIx,
                                    tscStartTime :: ClockTime,
                                    tscFrequency :: Int64,
                                    tscColumns :: [(ColumnName, ColumnType)]
                                  }
        deriving (Show)

    data QueryState = QueryState {
                          queryKey :: QueryKey,
                          queryServerName :: String,
                          queryZMQContext :: ZMQ.Context
                      }
                deriving Typeable

    getZMQContext :: GMachine ZMQ.Context
    getZMQContext = do
      userState <- liftM gmachineUserState get
      let queryState = fromDyn userState (error "Bad state for DIME GMachine") :: QueryState
      return $ queryZMQContext queryState