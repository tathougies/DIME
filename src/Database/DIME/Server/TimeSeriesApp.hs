{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}
module Database.DIME.Server.TimeSeriesApp (timeSeriesApp, listAllTimeSeries) where

import System.Log.Logger

import Network.Wai
import Network.Wai.Parse
import Network.HTTP.Types
import Network.HTTP.Types.URI
import Network.Wai.Handler.Warp (run)

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad
import Control.Monad.Trans.Resource

import Data.ByteString.Lazy.Char8 ()
import qualified Data.ByteString.Lazy.Char8 as LBS
import qualified Data.ByteString.Char8 as BS
import qualified Data.Text as T
import qualified Data.Map as M
import Data.String

import Database.DIME.Server.HttpError
import Database.DIME.Server.Util
import Database.DIME.Server.State
import Database.DIME.Memory.Block

import Text.JSON

moduleName = "Database.DIME.Server.TimeSeriesApp"

listAllTimeSeries :: State -> Application
listAllTimeSeries state request = do
  tsNames <- liftIO $ atomically $ do
    tssMap <- readTVar $ timeSeriessRef state
    let tsVars = M.elems tssMap
    tss <- mapM readTVar tsVars
    return $ map getName tss
  ok [contentTypeJson] $ fromString $ encode tsNames

timeSeriesApp :: State -> TimeSeriesName -> [T.Text] -> Application
timeSeriesApp state timeSeriesName path request =
    case (pathHead path) of
      Nothing -> index state timeSeriesName path request
      Just "_data" -> dataPath state timeSeriesName path request
      Just columnName -> columnPath state timeSeriesName (ColumnName columnName) path request
  where
    index :: State -> TimeSeriesName -> [T.Text] -> Application
    index state timeSeriesName path request =
        case method request of
          Right PUT -> do
            let queryVars = queryString request
            case lookup "frequency" queryVars of
              Just (Just frequency) ->
                  do
                    let maxNullTime = case lookup "maxNullTime" queryVars of
                                        Just Nothing -> Nothing
                                        Nothing -> Nothing
                                        Just (Just x) -> Just (read $ BS.unpack x)
                    timeSeries <- liftIO $ newTimeSeries timeSeriesName (read $ BS.unpack frequency) maxNullTime state
                    jsonRepr <- atomicallyR $ timeSeriesAsJSON timeSeries -- Create new time series, and display the information
                    ok [contentTypeJson] jsonRepr
              _ -> badRequest
          Right GET -> do
            result <- atomicallyR $ lookupTimeSeries timeSeriesName state
            case result of
              Nothing -> notFound
              Just timeSeries -> do
                          jsonRepr <- atomicallyR $ timeSeriesAsJSON timeSeries
                          ok [contentTypeJson] jsonRepr
          _ -> badMethod

    dataPath state timeSeriesName path request =
        case method request of
          Right PUT -> badRequest -- Update request
          Right POST -> do -- Append data request
            postData <- sourceToBS $ requestBody request
            let rowData = rowDataFromPostData postData
            timeSeriesR <- atomicallyR $ lookupTimeSeries timeSeriesName state
            case timeSeriesR of
              Nothing -> notFound
              Just timeSeries ->
                  do
                    timeSeriesData <- liftIO $ readTVarIO timeSeries
                    let columns = M.elems $ getColumns timeSeriesData
                    columnsData <- mapM (liftIO.readTVarIO) columns
                    let columnsAndValues = map lookupColumnInData columnsData
                        lookupColumnInData column =
                             let ColumnName columnName' = getColumnName column
                                 columnName = T.unpack columnName'
                                 columnValue =
                                     do
                                       columnJSValue <- lookup columnName rowData
                                       -- Determine column type and try to
                                       jsValueToColumnValue (getColumnType column) columnJSValue
                             in (getColumnName column, columnValue)

                        -- Make sure required columns are filled in
                        requiredColumnsFilled = all checkFilled columnsAndValues
                        checkFilled (column, Nothing) = False -- TODO allow null columns
                        checkFilled (_, Just _) = True
                    if requiredColumnsFilled
                     then do
                       let columnsAndJustValues = map (\(a, Just b) -> (a, b)) columnsAndValues
                       result <- liftIO $ appendRow timeSeriesName columnsAndJustValues state -- actually add everything
                       case result of
                         Just index -> ok [] $ LBS.pack $ "Row added successfully at " ++ show index
                         Nothing -> internalServerError
                     else badRequest
          Right GET -> badRequest -- Query
          _ -> badMethod

    rowDataFromPostData postData =
        failOnError (decode $ BS.unpack postData)
            (\rowData ->
             case rowData of
               JSObject jsObj -> fromJSObject jsObj
               _ -> fail "Need JSObject as postData"
            )

    columnPath state timeSeriesName columnName path request =
        case method request of
          Right PUT -> do
            let queryVars = queryString request
            case lookup "type" queryVars of
              Nothing -> badRequest -- No type parameter or a parameterless type parameter are both bad!
              Just Nothing -> badRequest
              Just (Just typeStr) ->
                  do
                    let columnType = stringToColumnType typeStr
                    column <- liftIO $ newColumn timeSeriesName columnName columnType state
                    jsonRepr <- atomicallyR $ columnAsJSON column
                    ok [contentTypeJson] jsonRepr
          Right GET -> do
            result <- atomicallyR $ lookupColumn timeSeriesName columnName state
            case result of
              Nothing -> notFound
              Just column ->
                  do
                    jsonRepr <- atomicallyR $ columnAsJSON column
                    ok [contentTypeJson] jsonRepr
          Right DELETE -> do
            liftIO $ deleteColumn timeSeriesName columnName state
            ok [] "Deleted column"
          _ -> badMethod

    stringToColumnType typeStr
                        | typeStr == "int" = IntColumn
                        | typeStr == "string" = StringColumn
                        | typeStr == "double" = DoubleColumn
                        | otherwise = error "Bad column type given"