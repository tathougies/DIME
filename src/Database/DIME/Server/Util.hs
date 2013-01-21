{-# LANGUAGE OverloadedStrings #-}
module Database.DIME.Server.Util
    (pathHead, liftSTM, atomicallyR,
     normalizedPath, method, sourceToLBS,
     failOnError
     ) where

import Control.Monad
import Control.Monad.IO.Class
import Control.Concurrent.STM

import Data.Text
import Data.Maybe
import Data.String
import Data.Conduit
import Data.Conduit.Util
import qualified Data.ByteString as BS

import Network.Wai
import Network.HTTP.Types

import System.Time

import Text.JSON

pathHead :: [Text] -> Maybe Text
pathHead [] = Nothing
pathHead [""] = Nothing
pathHead (x:xs) = Just x

atomicallyR, liftSTM :: MonadIO m => STM a -> m a
liftSTM = liftIO.atomically
atomicallyR = liftSTM

normalizedPath :: [Text] -> [Text]
normalizedPath (x:"..":xs) = normalizedPath xs
normalizedPath (".":xs) = normalizedPath xs
normalizedPath (x:xs) = x : normalizedPath xs
normalizedPath [] = []

method = parseMethod.requestMethod

sourceToLBS :: Monad m => Source m BS.ByteString -> m BS.ByteString
sourceToLBS s = s $$ (sinkState BS.empty append finish)
    where
      append a s = return $ StateProcessing $ BS.append a s
      finish = return

failOnError :: Result a -> (a -> b) -> b
failOnError (Error e) _ = error e
failOnError (Ok a) f = f a

-- diffLocalTime :: LocalTime -> LocalTime -> DiffTime
-- diffLocalTime a b = let daysBetween = (localDay a) `diffDays` (localDay b)
--                         timeDiffSeconds = (timeOfDayToTime $ localTimeOfDay a) - (timeOfDayToTime $ localTimeOfDay b)
--                     in (fromIntegral daysBetween) * 86400 + timeDiffSeconds