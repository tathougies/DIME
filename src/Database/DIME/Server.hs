{-# LANGUAGE OverloadedStrings #-}
module Database.DIME.Server
    (
     serverMain
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad.Trans
import Control.Monad

import qualified Data.ByteString.Char8 as BS
import qualified Data.Text as T
import qualified Data.ByteString.Lazy.Char8 as LBS
import qualified Data.Binary as Bin
import qualified Data.Map as M

import Database.DIME.Server.TimeSeriesApp
import Database.DIME.Server.Util
import Database.DIME.Server.HttpError
import Database.DIME.Server.State
import Database.DIME.Server.Peers
import Database.DIME.Server.Config
import Database.DIME.Transport
import Database.DIME.DataServer.Command as DataCommand
import Database.DIME.DataServer.Response as DataResponse
import Database.DIME.Util

import Network.Wai
import Network.HTTP.Types
import Network.Wai.Handler.Warp (run)

import System.Log.Logger
import System.Time
import System.Locale

import qualified Text.JSON as J

type HttpPath = [T.Text]

dumpDelay = 60000000 -- dump every minute!

moduleName = "Database.DIME.Server"

prefsApp :: HttpPath -> Application
prefsApp timeSeriesName request = return $ responseLBS status400 [("Content-Type", "text/plain")] ""

queryEndpoint :: PeerName -> SockAddr
queryEndpoint (PeerName peer) =
    case peer of
      SockAddrInet _ host -> SockAddrInet (fromIntegral queryBrokerPort) host
      SockAddrInet6 _ flow host scope -> SockAddrInet6 (fromIntegral queryBrokerPort) flow host scope
      SockAddrUnix file -> SockAddrUnix (file ++ "_query")

queryApp :: State -> HttpPath -> Application
queryApp st [] request = do
  -- Run query!
  postData <- sourceToBS $ requestBody request

  let queryText = T.pack $ BS.unpack $ postData
      queryCmd = RunQuery (QueryKey 0) queryText

      formatTime calTime = (formatCalendarTime defaultTimeLocale "%Y-%m-%d %H:%M:%S." calTime) ++ show (ctPicosec calTime `div` 1000000)

  peers <- liftIO (M.keys . getPeers <$> (atomically . readTVar . peersRef $ st))
  peer <- liftIO (pick peers)
  liftIO $ sendRequest (zmqContext st) (Connect (queryEndpoint peer)) queryCmd $
      \response ->
        case response of
          QueryResponse (DoubleResult d) -> ok [contentTypeJson] $ LBS.pack $ J.encode d
          QueryResponse (IntResult i) -> ok [contentTypeJson] $ LBS.pack $ J.encode i
          QueryResponse (StringResult t) -> ok [contentTypeJson] $ LBS.pack $ J.encode t
          QueryResponse (TimeSeriesResult tsData) -> do
              adjData <- mapM (\(cTime, dat) -> do
                                   calTime <- toCalendarTime cTime
                                   return $ (formatTime calTime, dat)) tsData
              ok [contentTypeJson] $ LBS.pack $ J.encode adjData
          Fail e -> badRequest' (LBS.pack e)
          _ -> internalServerError

queryApp _ _ _ = badRequest -- shouldn't have any more path components

webServerApp :: State -> Application
webServerApp state req = let path = normalizedPath $ pathInfo req
                         in if length path == 0 then listAllTimeSeries state req
                            else case head path of
                                   "_prefs" -> prefsApp (tail path) req
                                   "_query" -> queryApp state (tail path) req
                                   timeSeriesName -> timeSeriesApp state (TimeSeriesName timeSeriesName) (tail path) req

webServerMain :: State -> IO ()
webServerMain state = do
  infoM moduleName "DIME server starting (debug mode on) ..."
  infoM moduleName ("Running on " ++ show webPort)

  run webPort (webServerApp state)

dumpStatePeriodically :: State -> IO ()
dumpStatePeriodically state = forever $ do
                                threadDelay dumpDelay
                                rebuildTimeSeriesMap state

serverMain :: IO ()
serverMain = withContext $ \ctxt -> do
  serverState <- newServerState ctxt "timeSeriesData.json"
  forkIO $ webServerMain serverState
  forkIO $ dumpStatePeriodically serverState
  peerServer serverState
  return ()