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

splice :: Int -> [a] -> [a]
splice i xs = let (init, _:tail) = splitAt i xs
              in init ++ tail