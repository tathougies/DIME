{-# LANGUAGE ScopedTypeVariables, TypeFamilies #-}
module Database.DIME.Transport
    (EndPoint(..),

     -- safeZmq,
     -- safeConnect, safeBind, safeSend, safeAttach,

     MonadTransport(..),

     attach,

     withAttachedSocket,
     sendRequest, sendOneRequest, serveRequest,

     ipv4, bindingPort
    ) where

import qualified Control.Exception as E
import Control.Monad (forever)
import Control.Monad.Trans
import Control.Applicative

import qualified Data.ByteString as Strict
import qualified Data.List.NonEmpty as NonEmpty
import Data.ByteString.Lazy
import Data.Binary
import Data.Maybe

import Foreign.C.Error

import GHC.IO.Exception
import GHC.Conc

import qualified Network.Socket as Net hiding (send, recv)
import qualified Network.Socket.ByteString as NetBS

import System.IO.Error
import System.IO.Unsafe

data EndPoint = Connect Net.SockAddr |
                Bind Net.SockAddr

bindingPort :: Net.PortNumber -> EndPoint
bindingPort portNo = Bind (ipv4 portNo "")

ipv4 :: Net.PortNumber -> String -> Net.SockAddr
ipv4 portNo "" = Net.SockAddrInet portNo Net.iNADDR_ANY
ipv4 portNo addr = unsafePerformIO $ do
  addr <- Prelude.head <$> Net.getAddrInfo (Just Net.defaultHints { Net.addrFlags = [Net.AI_ADDRCONFIG, Net.AI_CANONNAME] }) (Just addr) (Just (show portNo))
  return (Net.addrAddress addr)

intrErrNo = let Errno e = eINTR
            in fromIntegral e

class (Monad m, MonadIO m) => MonadTransport m where
    data Context m
    data Socket m

    -- initTransport :: m (Context m)
    withContext :: (Context m -> m a) -> m a

    connect :: Socket m -> Net.SockAddr -> m ()
    bind :: Socket m -> Net.SockAddr -> m ()

    send :: Socket m -> Strict.ByteString -> m ()
    send' :: Socket m -> ByteString -> m ()

    receive :: Socket m -> m Strict.ByteString
    accept :: Socket m -> m (Socket m, Net.SockAddr)

    withSocket :: Context m -> (Socket m -> m b) -> m b

instance MonadTransport IO where
  data Context IO = Context
  newtype Socket IO = SocketIO Net.Socket

  withContext a = Net.withSocketsDo (a Context)

  connect (SocketIO s) addr = safeIO $ Net.connect s addr
  bind (SocketIO s) addr = do
    safeIO $ Net.bindSocket s addr
    safeIO $ Net.listen s 5
  send (SocketIO s) dat = do
    let datLength = Strict.length dat
        lengthEncoded = encode (fromIntegral datLength :: Word16)
    safeIO $ NetBS.send s (Strict.concat . toChunks $ lengthEncoded)
    safeIO $ NetBS.send s dat
    return ()
  send' s datLazy = send s (Strict.concat . toChunks $ datLazy)
  receive (SocketIO s) = do
    lengthData <- safeIO $ NetBS.recv s 2
    let length = decode (fromChunks [lengthData]) :: Word16
    dat <- safeIO $ NetBS.recv s (fromIntegral length)
    return dat
  accept (SocketIO s) = do
    (s', a) <- Net.accept s
    return (SocketIO s', a)
  withSocket Context action =
    E.bracket (SocketIO <$> Net.socket Net.AF_INET Net.Stream Net.defaultProtocol)
              (\(SocketIO s) -> safeIO $ Net.close s)
              action

safeIO :: IO a -> IO a
safeIO ioAction =
  E.catch ioAction $
      \(e :: IOError) ->
          if isJust (ioe_errno e) &&
             (fromJust . ioe_errno $ e) == intrErrNo
          then safeIO ioAction
          else E.throw e

attach :: MonadTransport m => Socket m -> EndPoint -> m ()
attach s (Connect peer) = connect s peer
attach s (Bind interface) = bind s interface
-- attach s (ConnectBind peer interface) = connect s peer >> bind s interface

-- sendRequest :: (Binary requestType, Binary responseType, MonadTransport m) => Context m -> EndPoint -> requestType -> (responseType -> m a) -> m a
-- sendRequest c endPoint request responseHandler=
--     safelyWithSocket c ZMQ.Req endPoint $ \s ->
--         sendOneRequest s request responseHandler

-- sendOneRequest :: (Binary requestType, Binary responseType, ZMQ.Sender s, ZMQ.Receiver s, MonadTransport m) => Socket m s -> requestType -> (responseType -> m a) -> m a
-- sendOneRequest s request responseHandler = do
--   let cmdData = encode request
--   send' s [] cmdData
--   reply <- receive s
--   let response = decode $ fromChunks [reply]
--   responseHandler response

serveRequest :: (Binary requestType, Binary responseType, MonadTransport m) => Socket m -> m () -> (requestType -> m responseType) -> m ()
serveRequest s afterResp reqHandler = do
  line <- receive s
  let cmd = decode $ fromChunks [line]
  response <- reqHandler cmd
  send' s (encode response)
  afterResp

-- safelyWithSocket :: (ZMQ.SocketType a, MonadTransport m) => Context m -> a -> EndPoint -> (Socket m a -> m b) -> m b
-- safelyWithSocket c socketType endPoint handler =
--     withSocket c socketType $ \s -> do
--       attach s endPoint
--       handler s

-- -- MonadTransport instances

-- instance MonadTransport IO where
--     newtype Context IO = ZMQContext ZMQ.Context
--     newtype Socket IO a = ZMQSocket (ZMQ.Socket a)

--     initTransport = do
--       c <- ZMQ.init $ fromIntegral numCapabilities
--       return $ ZMQContext c
--     withContext ctxtHandler = ZMQ.withContext (ctxtHandler . ZMQContext)

--     connect (ZMQSocket s) peer = safeZmq (ZMQ.connect s peer)
--     bind (ZMQSocket s) interface = safeZmq (ZMQ.bind s interface)

--     send (ZMQSocket s) flags dat = safeZmq (ZMQ.send s flags dat)
--     send' (ZMQSocket s) flags dat = safeZmq (ZMQ.send' s flags dat)

--     receive (ZMQSocket s) = safeZmq (ZMQ.receive s)

--     withSocket (ZMQContext c) sockType socketHandler = ZMQ.withSocket c sockType (socketHandler . ZMQSocket)

-- safeZmq :: IO a -> IO a
-- safeZmq zmqAction =
--     E.catch zmqAction $
--          (\(e :: ZMQ.ZMQError) ->
--               if (ZMQ.errno e) == intrErrNo then safeZmq zmqAction else E.throw e)

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

-- safelyWithSocket :: (ZMQ.SocketType a) => ZMQ.Context -> a -> EndPoint -> (ZMQ.Socket a -> IO b) -> IO b
-- safelyWithSocket c socketType endPoint handler =
--   withSocket c socketType $ \s -> do
--     safeAttach s endPoint
--       handler s

withAttachedSocket :: (MonadTransport m) => Context m -> EndPoint -> (Socket m -> m a) -> m a
withAttachedSocket ctxt endPoint action =
  withSocket ctxt $ \s -> do
    attach s endPoint
    action s

sendRequest :: (MonadTransport m, Binary requestType, Binary responseType) => Context m -> EndPoint -> requestType -> (responseType -> m a) -> m a
sendRequest c endPoint request responseHandler =
    withAttachedSocket c endPoint $ \s ->
      sendOneRequest s request responseHandler

sendOneRequest :: (MonadTransport m, Binary requestType, Binary responseType) => Socket m -> requestType -> (responseType -> m a) -> m a
sendOneRequest s request responseHandler = do
    let cmdData = encode $ request
    send' s cmdData
    reply <- receive s
    let response = decode $ fromChunks [reply]
    responseHandler response

-- serveRequest :: (Binary requestType, Binary responseType, ZMQ.Sender s, ZMQ.Receiver s) => ZMQ.Socket s -> IO () -> (requestType -> IO responseType) -> IO ()
-- serveRequest s afterResp reqHandler = do
--   line <- safeReceive s
--   let cmd = decode $ fromChunks [line]
--   response <- reqHandler cmd
--   safeSend' s [] $ encode response
--   afterResp

-- proxy :: (ZMQ.Receiver from, ZMQ.Sender from, ZMQ.Sender to, ZMQ.Receiver to) => Socket IO from -> Socket IO to -> IO ()
-- proxy from to = do
--   forkIO $ forever $ tunnel to from
--   forever $ tunnel from to

-- tunnel :: (ZMQ.Receiver a, ZMQ.Sender b) => Socket IO a -> Socket IO b -> IO ()
-- tunnel (ZMQSocket from) (ZMQSocket to) = ZMQ.receiveMulti from >>= ZMQ.sendMulti to . NonEmpty.fromList

-- tunnelMsg :: (ZMQ.Receiver a, ZMQ.Sender b) => String -> Socket IO a -> Socket IO b -> IO ()
-- tunnelMsg msg (ZMQSocket from) (ZMQSocket to) = do
--   parts <- ZMQ.receiveMulti from
--   Prelude.putStrLn msg
--   ZMQ.sendMulti to . NonEmpty.fromList $ parts
