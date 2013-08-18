module Database.DIME.Util where

import Prelude hiding (catch)
import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.MVar
import Control.Exception

import Data.Maybe

import System.Log.Logger
import System.Random (randomRIO)

pick :: [a] -> IO a
pick xs = randomRIO (0, length xs - 1) >>= return . (xs !!)

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

mapP :: (a -> IO b) -> [a] -> IO [b]
mapP f xs = do
  results <- mapM (const newEmptyMVar) xs
  forM_ (zip xs results) $
       \(x, resultVar) -> (do
          result <- f x
          putMVar resultVar (Right result)) `catch` ((putMVar resultVar) . Left)
  forM results $
       \resultVar ->
           takeMVar resultVar >>= \result ->
               case result of
                 Left e -> throw (e :: SomeException)
                 Right x -> return x