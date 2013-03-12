{-# LANGUAGE ScopedTypeVariables, TypeFamilies #-}
module Database.DIME.Transport
    (EndPoint(..),
     ZMQ.SocketType,
     ZMQ.Req(..), ZMQ.Rep(..),
     ZMQ.Dealer(..), ZMQ.Router(..),
     ZMQ.Pub(..), ZMQ.Sub(..),

     -- safeZmq,
     -- safeConnect, safeBind, safeSend, safeAttach,

     MonadTransport(..),

     attach,

     safelyWithSocket,
     sendRequest, sendOneRequest, serveRequest,

     proxy, tunnel
    ) where

import qualified Control.Exception as E
import Control.Monad (forever)
import Control.Monad.Trans

import qualified Data.ByteString as Strict
import Data.ByteString.Lazy
import Data.Binary

import Foreign.C.Error

import GHC.IO.Exception
import GHC.Conc

import qualified System.ZMQ3 as ZMQ
import System.IO.Error

data EndPoint = Connect String |
                Bind String |
                ConnectBind String String

intrErrNo :: Int
intrErrNo = let Errno e = eINTR
            in fromIntegral e

class (Monad m, MonadIO m) => MonadTransport m where
    data Context m

    data Socket m a

    initTransport :: m (Context m)
    withContext :: (Context m -> m a) -> m a

    connect :: Socket m a -> String -> m ()
    bind :: Socket m a -> String -> m ()

    send :: ZMQ.Sender a => Socket m a -> [ZMQ.Flag] -> Strict.ByteString -> m ()
    send' :: ZMQ.Sender a => Socket m a -> [ZMQ.Flag] -> ByteString -> m ()

    receive :: ZMQ.Receiver a => Socket m a -> m Strict.ByteString

    withSocket :: ZMQ.SocketType a => Context m -> a -> (Socket m a -> m b) -> m b

attach :: MonadTransport m => Socket m a -> EndPoint -> m ()
attach s (Connect peer) = connect s peer
attach s (Bind interface) = bind s interface
attach s (ConnectBind peer interface) = connect s peer >> bind s interface

sendRequest :: (Binary requestType, Binary responseType, MonadTransport m) => Context m -> EndPoint -> requestType -> (responseType -> m a) -> m a
sendRequest c endPoint request responseHandler=
    safelyWithSocket c ZMQ.Req endPoint $ \s ->
        sendOneRequest s request responseHandler

sendOneRequest :: (Binary requestType, Binary responseType, ZMQ.Sender s, ZMQ.Receiver s, MonadTransport m) => Socket m s -> requestType -> (responseType -> m a) -> m a
sendOneRequest s request responseHandler = do
  let cmdData = encode request
  send' s [] cmdData
  reply <- receive s
  let response = decode $ fromChunks [reply]
  responseHandler response

serveRequest :: (Binary requestType, Binary responseType, ZMQ.Sender s, ZMQ.Receiver s, MonadTransport m) => Socket m s -> m () -> (requestType -> m responseType) -> m ()
serveRequest s afterResp reqHandler = do
  line <- receive s
  let cmd = decode $ fromChunks [line]
  response <- reqHandler cmd
  send' s [] $ encode response
  afterResp

safelyWithSocket :: (ZMQ.SocketType a, MonadTransport m) => Context m -> a -> EndPoint -> (Socket m a -> m b) -> m b
safelyWithSocket c socketType endPoint handler =
    withSocket c socketType $ \s -> do
      attach s endPoint
      handler s

-- MonadTransport instances

instance MonadTransport IO where
    newtype Context IO = ZMQContext ZMQ.Context
    newtype Socket IO a = ZMQSocket (ZMQ.Socket a)

    initTransport = do
      c <- ZMQ.init $ fromIntegral numCapabilities
      return $ ZMQContext c
    withContext ctxtHandler = ZMQ.withContext (ctxtHandler . ZMQContext)

    connect (ZMQSocket s) peer = safeZmq (ZMQ.connect s peer)
    bind (ZMQSocket s) interface = safeZmq (ZMQ.bind s interface)

    send (ZMQSocket s) flags dat = safeZmq (ZMQ.send s flags dat)
    send' (ZMQSocket s) flags dat = safeZmq (ZMQ.send' s flags dat)

    receive (ZMQSocket s) = safeZmq (ZMQ.receive s)

    withSocket (ZMQContext c) sockType socketHandler = ZMQ.withSocket c sockType (socketHandler . ZMQSocket)

safeZmq :: IO a -> IO a
safeZmq zmqAction =
    E.catch zmqAction $
         (\(e :: ZMQ.ZMQError) ->
              if (ZMQ.errno e) == intrErrNo then safeZmq zmqAction else E.throw e)

-- safeConnect :: ZMQ.Socket a -> String -> IO ()
-- safeConnect s peer = safeZmq (ZMQ.connect s peer)

-- safeBind :: ZMQ.Socket a -> String -> IO ()
-- safeBind s interface = safeZmq (ZMQ.bind s interface)

-- safeSend :: ZMQ.Sender a => ZMQ.Socket a -> [ZMQ.Flag] -> Strict.ByteString -> IO ()
-- safeSend s flags dat = safeZmq (ZMQ.send s flags dat)

-- safeSend' :: ZMQ.Sender a => ZMQ.Socket a -> [ZMQ.Flag] -> ByteString -> IO ()
-- safeSend' s flags dat = safeZmq (ZMQ.send' s flags dat)

-- safeReceive :: ZMQ.Receiver a => ZMQ.Socket a -> IO Strict.ByteString
-- safeReceive s = safeZmq (ZMQ.receive s)

-- safeAttach :: ZMQ.Socket a -> EndPoint -> IO ()
-- safeAttach s (Connect peer) = safeConnect s peer
-- safeAttach s (Bind interface) = safeBind s interface
-- safeAttach s (ConnectBind peer interface) = safeConnect s peer >> safeBind s interface

-- sendRequest :: (Binary requestType, Binary responseType) => ZMQ.Context -> EndPoint -> requestType -> (responseType -> IO a) -> IO a
-- sendRequest c endPoint request responseHandler =
--     safelyWithSocket c ZMQ.Req endPoint $ \s ->
--       sendOneRequest s request responseHandler

-- safelyWithSocket :: (ZMQ.SocketType a) => ZMQ.Context -> a -> EndPoint -> (ZMQ.Socket a -> IO b) -> IO b
-- safelyWithSocket c socketType endPoint handler =
--     ZMQ.withSocket c socketType $ \s -> do
--       safeAttach s endPoint
--       handler s

-- sendOneRequest :: (Binary requestType, Binary responseType, ZMQ.Sender s, ZMQ.Receiver s) => ZMQ.Socket s -> requestType -> (responseType -> IO a) -> IO a
-- sendOneRequest s request responseHandler = do
--     let cmdData = encode $ request
--     safeSend' s [] cmdData
--     reply <- safeReceive s
--     let response = decode $ fromChunks [reply]
--     responseHandler response

-- serveRequest :: (Binary requestType, Binary responseType, ZMQ.Sender s, ZMQ.Receiver s) => ZMQ.Socket s -> IO () -> (requestType -> IO responseType) -> IO ()
-- serveRequest s afterResp reqHandler = do
--   line <- safeReceive s
--   let cmd = decode $ fromChunks [line]
--   response <- reqHandler cmd
--   safeSend' s [] $ encode response
--   afterResp

proxy :: (ZMQ.Receiver from, ZMQ.Sender from, ZMQ.Sender to, ZMQ.Receiver to) => Socket IO from -> Socket IO to -> IO ()
proxy from to = do
  forkIO $ forever $ tunnel to from
  forever $ tunnel from to

tunnel :: (ZMQ.Receiver a, ZMQ.Sender b) => Socket IO a -> Socket IO b -> IO ()
tunnel (ZMQSocket from) (ZMQSocket to) = do
  parts <- ZMQ.receiveMulti from
  ZMQ.sendMulti to parts
