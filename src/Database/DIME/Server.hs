{-# LANGUAGE OverloadedStrings #-}
module Database.DIME.Server
    (
     serverMain
    ) where

import System.Log.Logger

import Control.Concurrent
import Control.Monad.IO.Class

import Network.Wai
import Network.HTTP.Types
import Network.Wai.Handler.Warp (run)

import Data.ByteString.Lazy.Char8 ()
import qualified Data.Text as T

import Database.DIME.Server.TimeSeriesApp
import Database.DIME.Server.Util
import Database.DIME.Server.HttpError
import Database.DIME.Server.State
import Database.DIME.Server.Peers

type HttpPath = [T.Text]

moduleName = "Database.DIME.Server"

prefsApp :: HttpPath -> Application
prefsApp timeSeriesName request = return $ responseLBS status400 [("Content-Type", "text/plain")] ""

webServerApp :: State -> Application
webServerApp state req = let path = normalizedPath $ pathInfo req
                         in if length path == 0 then badRequest
                            else case head path of
                                   "_prefs" -> prefsApp (tail path) req
                                   timeSeriesName -> timeSeriesApp state (TimeSeriesName timeSeriesName) (tail path) req

webServerMain :: State -> IO ()
webServerMain state = do
  infoM moduleName "DIME server starting..."
  debugM moduleName "Welcome to DIME web server debug client"

  run 8000 (webServerApp state)

serverMain :: IO ()
serverMain = do
  serverState <- newServerState "timeSeriesData.json"
  forkIO $ webServerMain serverState
  peerServer $ peersRef serverState
  return ()