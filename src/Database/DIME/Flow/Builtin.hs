{-# LANGUAGE ViewPatterns, RecordWildCards, OverloadedStrings #-}
module Database.DIME.Flow.Builtin where

    import Control.Monad.Trans
    import Control.Monad.State
    import Control.Monad

    import qualified Data.Map as M
    import qualified Data.Tree.DisjointIntervalTree as DIT
    import qualified Data.IntSet as IntSet
    import Data.Text hiding (map, intercalate)
    import Data.List
    import Data.String
    import Data.Int
    import Data.Array

    import qualified Database.DIME.Server.Peers as Peers
    import Database.DIME
    import Database.DIME.Transport
    import Database.DIME.Server.State
    import Database.DIME.Flow.Types
    import Database.DIME.Flow.TimeSeries
    import Database.DIME.Memory.Block (ColumnType(..), Block, fromListUnboxed)
    import Database.DIME.Memory.Operation.Mappable
    import Database.DIME.Memory.Operation.Collapsible

    import Language.Flow.Module
    import Language.Flow.Execution.Types
    import Language.Flow.Execution.GMachine
    import Language.Flow.Builtin

    import System.Time

    dimeBuiltin :: Module
    dimeBuiltin = mkModule "DIME"
                  [("timeSeries", mkBuiltin "timeSeries" 1 builtinTimeSeries),
                   ("mkTSAccessor", mkBuiltin' "mkTSAccessor" 2 builtinMkTSAccessor),

                   -- Map operation
                   ("mapTS", mkBuiltin' "mapTS" 3 builtinMapTS),

                   -- Collapse operation
                   ("differentiate", mkBuiltin "differentiate" 1 builtinDifferentiate)
                  ]
        where
          builtinTimeSeries [x] = do
            if isString x then do
                  tsc <- fetchTimeSeriesCollection $ TimeSeriesName $ asString x
                  returnPureGeneric tsc
             else error $ "Require string as time series name. Got " ++ show x
          builtinTimeSeries x = throwError $ "timeSeries requires one argument."

          builtinMkTSAccessor [accessor, timeSeriesCollection] = do
              elems <- readGDataList accessor
              gEvaluate timeSeriesCollection -- strict in both arguments
              timeSeriesCollectionD <- readGraph timeSeriesCollection
              when (not $ isTimeSeriesCollection timeSeriesCollectionD) $
                   throwError $ "Cannot access columns of " ++ (unpack $ withGenericData typeName timeSeriesCollectionD) ++ " object"
              let timeSeriesCollection = asTimeSeriesCollection timeSeriesCollectionD
              let elems' = map asString elems
              case elems' of
                [columnName] -> do
                            ts <- fetchTimeSeries timeSeriesCollection (ColumnName columnName) -- access one column
                            returnPureGeneric ts
                columnNames ->
                    returnPureGeneric $ StringConstant $ fromString $ "Get fields " ++
                                          (intercalate ", " $ map unpack columnNames) ++ " of " ++
                                          show timeSeriesCollectionD
          builtinMkTSAccessor _ = throwError $ "mkTSAccessor takes one argument"

          builtinMapTS [op, ts1, ts2] = do
              gEvaluate ts1 -- strict in all arguments...
              gEvaluate ts2
              gEvaluate op
              ts1D <- readGraph ts1
              ts2D <- readGraph ts2
              when ((not $ isTimeSeries ts1D) || (not $ isTimeSeries ts2D)) $
                   throwError $ "The second and third arguments to mapTS must be TimeSeries objects"

              gmachineState <- get
              staticState <- liftIO $ freezeState gmachineState

              let ts1TS = asTimeSeries ts1D
                  ts2TS = asTimeSeries ts2D

                  staticState' = staticState {
                                   gmachineFrozenCode = [PushLocation op, MkAp, MkAp, Eval, ProgramDone],
                                   gmachineFrozenDump = []
                                 }

                  initDataAddrs = map fst $ gmachineInitData gmachineState

              oldData <- mapM readGraph initDataAddrs
              mapM (uncurry writeGraph) $ gmachineInitData gmachineState -- write initial data
              nonReachablePoints <- findNonReachablePoints ([op] ++ gmachineFrozenInitData staticState')
              zipWithM writeGraph initDataAddrs oldData -- restore data

              let staticState'' = staticState' {
                                    gmachineFrozenGraph = (gmachineFrozenGraph staticState') // (map (\pt -> (pt, Hole)) nonReachablePoints),
                                    gmachineFrozenFreeCells = IntSet.fromList $ map fromIntegral nonReachablePoints
                                  }
                  mapOp = GMachineMap staticState'' op

              newTS <- mapTS mapOp [ts1TS, ts2TS]
              returnPureGeneric newTS

          builtinDifferentiate [ts1] = do
              when (not $ isTimeSeries ts1) $
                   throwError $ "Argument to differentiate must be a time series"
              let ts1TS = asTimeSeries ts1
                  collapseOp = case tsDataType ts1TS of
                                 IntColumn -> Convolve (fromListUnboxed [1, -1] :: Block Int)
                                 DoubleColumn -> Convolve (fromListUnboxed [1, -1] :: Block Double)
              case tsDataType ts1TS of
                StringColumn -> throwError "Cannot differentiate time series of type string"
                _ -> return ()
              newTS <- collapseTS collapseOp 2 ts1TS
              returnPureGeneric newTS

    fetchTimeSeriesCollection :: TimeSeriesName -> GMachine TimeSeriesCollection
    fetchTimeSeriesCollection tsName = do
      userState <- (getUserState :: GMachine QueryState)
      let key = queryKey userState
          ctxt = queryZMQContext userState
          serverName = queryServerName userState
          infoCmd = Peers.TimeSeriesInfo key tsName

      liftIO $ sendRequest ctxt (Connect serverName) infoCmd $
             (\response ->
                  case response of
                    Peers.TimeSeriesInfoResponse {..} -> do
                        return $ TimeSeriesCollection {
                                     tscName = tableName,
                                     tscTableId = tableId,
                                     tscLength = tableLength,
                                     tscStartTime = startTime,
                                     tscFrequency = dataFrequency,
                                     tscColumns = columnNamesAndTypes
                                   }
                    Peers.ObjectNotFound -> let TimeSeriesName txt = tsName
                                            in error $ "Could not find time series " ++ unpack txt
                    _ -> error $ "Invalid response from " ++ show serverName)

    isTimeSeriesCollection :: GenericGData -> Bool
    isTimeSeriesCollection = checkType (typeName (undefined :: TimeSeriesCollection))

    asTimeSeriesCollection :: GenericGData -> TimeSeriesCollection
    asTimeSeriesCollection = checkCoerce (typeName (undefined :: TimeSeriesCollection))

    instance GData TimeSeriesCollection where
        typeName _ = fromString $ "TimeSeriesCollection"

        constr _ = error "Cannot construct instances of TimeSeriesCollection from code"
        constrArgs _ = array (0,-1) []

        getField (unpack -> "name") coll = let TimeSeriesName name = tscName coll
                                           in returnGeneric $ StringConstant name
        getField (unpack -> "tableId") coll = returnGeneric $ IntConstant $ fromIntegral $ tscTableId coll
        getField (unpack -> "length") coll = returnGeneric $ IntConstant $ fromIntegral $ tscLength coll
        getField (unpack -> "startTime") coll = case tscStartTime coll of
                                                  TOD startTime _ -> returnGeneric $ IntConstant $ fromIntegral $ startTime
        getField (unpack -> "dataFrequency") coll = returnGeneric $ IntConstant $ fromIntegral $ tscFrequency coll
        getField f _ = throwError $ "Invalid field for TimeSeriesCollection: " ++ unpack f
