module Database.DIME.Util where

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