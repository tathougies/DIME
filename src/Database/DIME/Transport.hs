{-# LANGUAGE ScopedTypeVariables, TypeFamilies, DeriveDataTypeable, DoAndIfThenElse #-}
module Database.DIME.Transport
    (EndPoint(..),

     -- safeZmq,
     -- safeConnect, safeBind, safeSend, safeAttach,

     MonadTransport(..),
     Net.SockAddr(..),

     attach,

     withAttachedSocket,
     sendRequest, sendOneRequest, serveRequest,
     serveRequests,

     ipv4, bindingPort
    ) where

import qualified Control.Exception as E
import Control.Monad (forever, when, forM_)
import Control.Monad.Trans
import Control.Applicative

import qualified Data.ByteString as Strict
import qualified Data.List.NonEmpty as NonEmpty
import Data.ByteString.Lazy as Lazy
import Data.Binary
import Data.Maybe
import Data.Typeable

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

data TransportDisconnected = TransportDisconnected
  deriving (Show, Read, Typeable)

instance E.Exception TransportDisconnected

class (Monad m, MonadIO m) => MonadTransport m where
    data Context m
    data Socket m

    -- initTransport :: m (Context m)
    withContext :: (Context m -> m a) -> m a

    connect :: Socket m -> Net.SockAddr -> m ()
    bind :: Socket m -> Net.SockAddr -> m ()
    finish :: Socket m -> m ()

    send :: Socket m -> Strict.ByteString -> m ()
    send' :: Socket m -> ByteString -> m ()

    receive :: Socket m -> m (Maybe Strict.ByteString)
    accept :: Socket m -> m (Socket m, Net.SockAddr)

    withSocket :: Context m -> (Socket m -> m b) -> m b

chunkSize = 65536

instance MonadTransport IO where
  data Context IO = Context
  newtype Socket IO = SocketIO Net.Socket

  withContext a = Net.withSocketsDo (a Context)

  connect (SocketIO s) addr = safeIO $ Net.connect s addr
  bind (SocketIO s) addr = do
    Net.setSocketOption s Net.ReuseAddr 1
    safeIO $ Net.bindSocket s addr
    safeIO $ Net.listen s 5
  finish (SocketIO s) = Net.close s
  send s dat = send' s (Lazy.fromChunks [dat])
  send' (SocketIO s) datLazy = do
    let datLength = Lazy.length datLazy
        lengthEncoded = encode (fromIntegral datLength :: Word32)
    safeIO $ NetBS.send s (Strict.concat . toChunks $ lengthEncoded)
    doSend s (toChunks datLazy)
  receive (SocketIO s) = do
    lengthData <- safeIO $ NetBS.recv s 4
    if Strict.length lengthData == 0
    then return Nothing
    else do
      let length = decode (fromChunks [lengthData]) :: Word32
      dat <- doRecv s (fromIntegral length) Strict.empty
      return (Just dat)
  accept (SocketIO s) = do
    (s', a) <- Net.accept s
    return (SocketIO s', a)
  withSocket Context action =
    E.bracket (SocketIO <$> Net.socket Net.AF_INET Net.Stream Net.defaultProtocol)
              (\(SocketIO s) -> safeIO $ Net.close s)
              action

doRecv :: Net.Socket -> Int -> Strict.ByteString -> IO Strict.ByteString
doRecv s left a = do
  dat <- safeIO $ NetBS.recv s (min left chunkSize)
  if Strict.length dat < left
    then doRecv s (left - Strict.length dat) (Strict.append a dat)
    else return (Strict.append a dat)

doSend :: Net.Socket -> [Strict.ByteString] -> IO ()
doSend s [] = return ()
doSend s (x:xs) = do
  bytesSent <- safeIO $ NetBS.send s x
  if bytesSent == Strict.length x
  then doSend s xs
  else doSend s (Strict.drop bytesSent x:xs)

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
  case line of
    Nothing -> return ()
    Just line -> do
      let cmd = decode $ fromChunks [line]
      response <- reqHandler cmd
      send' s (encode response)
      afterResp

serveRequests :: (Binary requestType, Binary responseType, MonadTransport m) => Socket m -> (requestType -> m responseType) -> m ()
serveRequests s reqHandler = do
  line <- receive s
  case line of
    Just line -> do
      let cmd = decode $ fromChunks [line]
      response <- reqHandler cmd
      send' s (encode response)
      serveRequests s reqHandler
    Nothing -> return ()

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
    case reply of
      Just reply -> do
        let response = decode $ fromChunks [reply]
        responseHandler response
      Nothing -> fail "Remote hung up without sending response"

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
