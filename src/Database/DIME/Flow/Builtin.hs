{-# LANGUAGE ViewPatterns, RecordWildCards #-}
module Database.DIME.Flow.Builtin where

    import Control.Monad.Trans
    import Control.Monad

    import qualified Data.Map as M
    import qualified Data.Tree.DisjointIntervalTree as DIT
    import Data.Text hiding (map, intercalate)
    import Data.List
    import Data.String
    import Data.Int
    import Data.Array

    import qualified Database.DIME.Server.Peers as Peers
    import Database.DIME
    import Database.DIME.Server.State
    import Database.DIME.Flow.Types
    import Database.DIME.Flow.TimeSeries
    import Database.DIME.Memory.Block (ColumnType(..))

    import Language.Flow.Module
    import Language.Flow.Execution.Types
    import Language.Flow.Execution.GMachine
    import Language.Flow.Builtin

    import qualified System.ZMQ3 as ZMQ
    import System.Time

    dimeBuiltin :: Module
    dimeBuiltin = mkModule "DIME"
                  [("timeSeries", mkBuiltin 1 builtinTimeSeries),
                   ("mkTSAccessor", mkGeneric $ BuiltinFun 2 $ GCodeBuiltin builtinMkTSAccessor)]
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

    fetchTimeSeriesCollection :: TimeSeriesName -> GMachine TimeSeriesCollection
    fetchTimeSeriesCollection tsName = do
      userState <- (getUserState :: GMachine QueryState)
      let key = queryKey userState
          ctxt = queryZMQContext userState
          serverName = queryServerName userState

      liftIO $ ZMQ.withSocket ctxt ZMQ.Req
         (\s -> do
            ZMQ.connect s serverName
            ZMQ.send s [] $ Peers.mkTimeSeriesInfoCommand key tsName
            reply <- ZMQ.receive s
            let response = Peers.parsePeerResponse reply
            case response of
              Peers.TimeSeriesInfoResponse {..} -> do
                  liftIO $ putStrLn $ "columns " ++ show columnNamesAndTypes
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
              _ -> error $ "Invalid response from " ++ serverName)

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
