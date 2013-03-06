{-# LANGUAGE ScopedTypeVariables #-}
module Database.DIME.Transport
    (EndPoint(..),
     ZMQ.SocketType,
     ZMQ.Req(..), ZMQ.Rep(..),
     ZMQ.Dealer(..), ZMQ.Router(..),
     ZMQ.Pub(..), ZMQ.Sub(..),
     ZMQ.Context,
     ZMQ.withContext,
     initTransport,

     safeZmq,
     safeConnect, safeBind, safeSend, safeAttach,
     safelyWithSocket,
     sendRequest, sendOneRequest, serveRequest,

     proxy, tunnel
    ) where

import qualified Control.Exception as E
import Control.Monad (forever)

import qualified Data.ByteString as Strict
import Data.ByteString.Lazy
import Data.Binary

import Foreign.C.Error

import GHC.IO.Exception
import GHC.Conc

import qualified System.ZMQ3 as ZMQ
import System.IO.Error

initTransport = ZMQ.init $ fromIntegral numCapabilities

data EndPoint = Connect String |
                Bind String |
                ConnectBind String String

-- data LoopResult = Continue | Stop

-- socketLoop :: (Bin requestType, Bin responseType) => ZMQ.Socket -> EndPoint -> IO requestType -> (responseType -> IO LoopResult) -> IO ()
-- socketLoop s endPoint mkRequest looper = do
--   safeAttach s endPoint
--   cmdData <-  liftM (head . LBS.toChunks . encode) mkRequest

intrErrNo :: Int
intrErrNo = let Errno e = eINTR
            in fromIntegral e

safeZmq :: IO a -> IO a
safeZmq zmqAction =
    E.catch zmqAction $
         (\(e :: ZMQ.ZMQError) ->
              if (ZMQ.errno e) == intrErrNo then safeZmq zmqAction else E.throw e)

safeConnect :: ZMQ.Socket a -> String -> IO ()
safeConnect s peer = safeZmq (ZMQ.connect s peer)

safeBind :: ZMQ.Socket a -> String -> IO ()
safeBind s interface = safeZmq (ZMQ.bind s interface)

safeSend :: ZMQ.Sender a => ZMQ.Socket a -> [ZMQ.Flag] -> Strict.ByteString -> IO ()
safeSend s flags dat = safeZmq (ZMQ.send s flags dat)

safeSend' :: ZMQ.Sender a => ZMQ.Socket a -> [ZMQ.Flag] -> ByteString -> IO ()
safeSend' s flags dat = safeZmq (ZMQ.send' s flags dat)

safeReceive :: ZMQ.Receiver a => ZMQ.Socket a -> IO Strict.ByteString
safeReceive s = safeZmq (ZMQ.receive s)

safeAttach :: ZMQ.Socket a -> EndPoint -> IO ()
safeAttach s (Connect peer) = safeConnect s peer
safeAttach s (Bind interface) = safeBind s interface
safeAttach s (ConnectBind peer interface) = safeConnect s peer >> safeBind s interface

sendRequest :: (Binary requestType, Binary responseType) => ZMQ.Context -> EndPoint -> requestType -> (responseType -> IO a) -> IO a
sendRequest c endPoint request responseHandler =
    safelyWithSocket c ZMQ.Req endPoint $ \s ->
      sendOneRequest s request responseHandler

safelyWithSocket :: (ZMQ.SocketType a) => ZMQ.Context -> a -> EndPoint -> (ZMQ.Socket a -> IO b) -> IO b
safelyWithSocket c socketType endPoint handler =
    ZMQ.withSocket c socketType $ \s -> do
      safeAttach s endPoint
      handler s

sendOneRequest :: (Binary requestType, Binary responseType, ZMQ.Sender s, ZMQ.Receiver s) => ZMQ.Socket s -> requestType -> (responseType -> IO a) -> IO a
sendOneRequest s request responseHandler = do
    let cmdData = encode $ request
    safeSend' s [] cmdData
    reply <- safeReceive s
    let response = decode $ fromChunks [reply]
    responseHandler response

serveRequest :: (Binary requestType, Binary responseType, ZMQ.Sender s, ZMQ.Receiver s) => ZMQ.Socket s -> IO () -> (requestType -> IO responseType) -> IO ()
serveRequest s afterResp reqHandler = do
  line <- safeReceive s
  let cmd = decode $ fromChunks [line]
  response <- reqHandler cmd
  safeSend' s [] $ encode response
  afterResp

proxy :: (ZMQ.Receiver from, ZMQ.Sender from, ZMQ.Sender to, ZMQ.Receiver to) => ZMQ.Socket from -> ZMQ.Socket to -> IO ()
proxy from to = do
  forkIO $ forever $ tunnel to from
  forever $ tunnel from to

tunnel :: (ZMQ.Receiver a, ZMQ.Sender b) => ZMQ.Socket a -> ZMQ.Socket b -> IO ()
tunnel from to = do
  parts <- ZMQ.receiveMulti from
  ZMQ.sendMulti to parts
