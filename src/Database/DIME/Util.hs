module Database.DIME.Util where

import Control.Monad
import Control.Concurrent.STM

import Data.Maybe

import System.Log.Logger

runStatefulIOLoop :: a -> (a -> IO a)-> IO ()
runStatefulIOLoop initialValue action = do
  newValue <- action initialValue
  runStatefulIOLoop newValue action

setDebugMode :: IO ()
setDebugMode = do
  logger <- getRootLogger
  let logger' = setLevel DEBUG logger
  saveGlobalLogger logger'

mapMaybeM :: Monad m => (a -> m (Maybe b)) -> [a] -> m [b]
mapMaybeM _ [] = return []
mapMaybeM f (x:xs) = do
  res <- f x
  case res of
    Nothing -> mapMaybeM f xs
    Just a -> (mapMaybeM f xs) >>= (return . (a:))

-- withTVar :: TVar a -> (a -> STM b) -> STM b
-- withTVar tVar f = do
--   val <- readTVar tVar
--   f val

-- modifyTVar :: TVar a -> (a -> STM a) -> STM a
-- modifyTVar tVar f = do
--   val <- readTVar tVar
--   val' <- f val
--   writeTVar tVar val'
--   return val'

-- modifyTVar_ :: TVar a -> (a -> STM a) -> STM ()
-- modifyTVar_ tVar f = do
--   val <- readTVar tVar
--   val' <- f val
--   writeTVar tVar val'