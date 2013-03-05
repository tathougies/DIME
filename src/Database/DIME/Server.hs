{-# LANGUAGE OverloadedStrings #-}
module Database.DIME.Server
    (
     serverMain
    ) where

import Control.Concurrent
import Control.Monad.Trans
import Control.Monad

import qualified Data.ByteString.Char8 as BS
import qualified Data.Text as T
import qualified Data.ByteString.Lazy.Char8 as LBS
import qualified Data.Binary as Bin

import Database.DIME.Server.TimeSeriesApp
import Database.DIME.Server.Util
import Database.DIME.Server.HttpError
import Database.DIME.Server.State
import Database.DIME.Server.Peers
import Database.DIME.Server.Config
import Database.DIME.DataServer.Command as DataCommand
import Database.DIME.DataServer.Response as DataResponse
import Database.DIME.Util

import Network.Wai
import Network.HTTP.Types
import Network.Wai.Handler.Warp (run)

import qualified System.ZMQ3 as ZMQ
import System.Log.Logger

type HttpPath = [T.Text]

dumpDelay = 60000000 -- dump every minute!

moduleName = "Database.DIME.Server"

prefsApp :: HttpPath -> Application
prefsApp timeSeriesName request = return $ responseLBS status400 [("Content-Type", "text/plain")] ""

queryApp :: State -> HttpPath -> Application
queryApp st [] request = do
  -- Run query!
  postData <- sourceToBS $ requestBody request
  liftIO $ ZMQ.withSocket (zmqContext st) ZMQ.Req $ \s -> do
      infoM moduleName $ "Connecting to query broker..."
      let queryText = T.pack $ BS.unpack $ postData
          queryRequest = BS.concat $ LBS.toChunks $ Bin.encode $ RunQuery (QueryKey 0) queryText
      ZMQ.connect s "inproc://queries"
      ZMQ.send s [] $ queryRequest
      reply <- ZMQ.receive s
      let response = (Bin.decode $ LBS.fromChunks [reply] :: DataResponse.Response)
      case response of
        QueryResponse dat -> ok [] $ LBS.pack $ show dat
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
  infoM moduleName "DIME server starting..."
  debugM moduleName "Welcome to DIME web server debug client"

  run webPort (webServerApp state)

dumpStatePeriodically :: State -> IO ()
dumpStatePeriodically state = forever $ do
                                threadDelay dumpDelay
                                rebuildTimeSeriesMap state

queryDistributor :: ZMQ.Context -> IO () -- receives query requests and forwards them on to some node to handle them
queryDistributor c = do
  infoM moduleName $ "Query broker starting..."
  ZMQ.withSocket c ZMQ.Router $ \routerS -> do
      ZMQ.bind routerS "inproc://queries"
      ZMQ.withSocket c ZMQ.Dealer $ \dealerS -> do
          ZMQ.bind dealerS $ "tcp://*:" ++ show queryBrokerPort
          forever $ do
            forkIO $ do
                      parts <- ZMQ.receiveMulti dealerS
                      ZMQ.sendMulti routerS parts
            parts <- ZMQ.receiveMulti routerS
            ZMQ.sendMulti dealerS parts

serverMain :: IO ()
serverMain = do
  setDebugMode
  serverState <- newServerState "timeSeriesData.json"
  forkIO $ webServerMain serverState
  forkIO $ queryDistributor $ zmqContext serverState
  forkIO $ dumpStatePeriodically serverState
  peerServer serverState
  return ()