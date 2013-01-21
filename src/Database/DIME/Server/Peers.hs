{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Database.DIME.Server.Peers
    ( PeerName(..),
      PeersInfo(..),
      PeerInfo(..),
      PeerResponse(..),
      empty,
      getLeastUtilizedPeer,
      calcMeanBlockCount,
      calcBlockCountStDev,
      peerServer,

      mkUpdateCommand,
      parsePeerResponse
    ) where

import Control.Concurrent.STM
import Control.Monad

import qualified Data.Map as Map
import qualified Data.PSQueue as PSQ
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.PSQueue (Binding ((:->)))
import Data.Binary as B
import Data.Maybe
import Data.String

import Database.DIME.Util

import System.Log.Logger
import qualified System.ZMQ3 as ZMQ

import qualified Text.JSON as JSON

moduleName = "Database.DIME.Server.Peers"

newtype PeerName = PeerName String -- ZeroMQ peer name
    deriving (Show, Ord, Eq, IsString, JSON.JSON, Binary)

data PeerInfo = PeerInfo {
      getPeerName :: PeerName,
      getBlockCount :: Int -- The number of blocks that this peer is handling
    }

data PeersInfo = PeersInfo {
        getPeers :: Map.Map PeerName (TVar PeerInfo),
        getPeersUsage :: PSQ.PSQ PeerName Int
      }

data PeerCommand = UpdateInfo PeerName Int
                 deriving (Show, Eq)

data PeerResponse = Ok | InfoRequest

empty :: PeersInfo
empty = PeersInfo Map.empty PSQ.empty

-- | The peer server. This runs on the main server and keeps track of the peer state.
peerServer :: TVar PeersInfo -> IO ()
peerServer peersInfo = do
  infoM moduleName "DIME peer server starting"
  ZMQ.withContext $ \c ->
      ZMQ.withSocket c ZMQ.Rep $ \s ->
          do
            ZMQ.bind s "tcp://127.0.0.1:8009"
            forever $ serve peersInfo s
  where
    serve peersInfoVar s = do
      line <- ZMQ.receive s
      let UpdateInfo peerName blockCount = B.decode $ LBS.fromChunks [line]
      peerAdded <- atomically $ do
        peersInfo <- readTVar peersInfoVar

        -- Update or add the peer
        let peers = getPeers peersInfo
        (peerAdded, peerInfoVar) <- case Map.lookup peerName peers of
                         Just peerInfoVar -> do -- If it already exists, just update it
                             modifyTVar' peerInfoVar
                                         (\peerInfo -> peerInfo { getBlockCount = blockCount })
                             return (False, peerInfoVar)
                         Nothing -> do -- Make new TVar for this peer
                             peerVar <- newTVar $ PeerInfo { getPeerName = peerName,
                                                             getBlockCount = blockCount }
                             let peers' = Map.insert peerName peerVar peers
                                 peersInfo' = peersInfo { getPeers = peers' }
                             writeTVar peersInfoVar peersInfo'
                             return (True, peerVar)

        -- Update its placement in the priority queue
        peerData <- readTVar peerInfoVar
        let peerPriority = calcPeerPriority peerData
            peersUsage' = PSQ.alter (Just . const peerPriority) peerName $ getPeersUsage peersInfo
        modifyTVar' peersInfoVar (\x -> x { getPeersUsage = peersUsage' })

        return peerAdded
      when peerAdded $ putStrLn $ "Added peer " ++ show peerName
      ZMQ.send s [] $ head $ LBS.toChunks $ B.encode $ if peerAdded then InfoRequest else Ok

    calcPeerPriority = getBlockCount -- this is the function you'd change to give a new heuristic to how peers are ranked

getLeastUtilizedPeer :: PeersInfo -> IO (Maybe PeerName)
getLeastUtilizedPeer peersInfo = do
  let peersUsage = getPeersUsage peersInfo
      peers = getPeers peersInfo
  case PSQ.findMin peersUsage of
    Nothing -> return Nothing
    Just (k :-> p) -> do
      let peerVar = fromJust $ Map.lookup k peers
      peerData <- readTVarIO peerVar
      return $ Just $ getPeerName peerData

calcMeanBlockCount :: Floating a => PeersInfo -> IO a
calcMeanBlockCount state = do
    peers <- mapM readTVarIO $ Map.elems $ getPeers state
    let peerBlockCount = map (fromIntegral.getBlockCount) peers
    return $ (sum peerBlockCount) / (fromIntegral $ length peerBlockCount)

calcBlockCountStDev :: Floating a => PeersInfo -> IO a
calcBlockCountStDev state = do
    peers <- mapM readTVarIO $ Map.elems $ getPeers state
    let peerBlockCount = map (fromIntegral.getBlockCount) peers
    meanBlockCount <- calcMeanBlockCount state
    let variance = sum (map (\x -> (x - meanBlockCount) * (x - meanBlockCount)) peerBlockCount) /
                   (fromIntegral $ length peerBlockCount)
    return $ sqrt variance

parsePeerResponse :: BS.ByteString -> PeerResponse
parsePeerResponse reply = decode $ LBS.fromChunks [reply]

mkUpdateCommand :: PeerName -> Int -> BS.ByteString
mkUpdateCommand zmqName blockCount = let updateCmd = UpdateInfo zmqName blockCount
                                     in head $ LBS.toChunks $ encode updateCmd

instance Binary PeerCommand where
    get = liftM2 UpdateInfo get get
    put (UpdateInfo peerName blockCount) = do
      put peerName
      put blockCount

instance Binary PeerResponse where
    get = do
      token <- (get :: Get Int)
      case token of
        1 -> return Ok
        2 -> return InfoRequest
        _ -> fail "Bad code for PeerResponse"
    put Ok = put (1 :: Int)
    put InfoRequest = put (2 :: Int)