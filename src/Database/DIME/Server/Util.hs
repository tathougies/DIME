{-# LANGUAGE OverloadedStrings #-}
-- | Utility functions for the web server
module Database.DIME.Server.Util
    (pathHead, liftSTM, atomicallyR,
     normalizedPath, method, sourceToBS,
     failOnError
     ) where

import Control.Monad
import Control.Monad.IO.Class
import Control.Concurrent.STM

import Data.Text hiding (reverse)
import Data.Maybe
import Data.String
import Data.Conduit
import Data.Conduit.Text as CT
import qualified Data.ByteString as BS

import Network.Wai
import Network.HTTP.Types

import System.Time

import Text.JSON

-- | Smart helper function to get path head. It's possible with HTTP paths from the Wai, this
-- function returns @Nothing@, if there are now more path elements to process or @Just x@ if @x@ is
-- the next path element
pathHead :: [Text] -> Maybe Text
pathHead [] = Nothing
pathHead [""] = Nothing
pathHead (x:xs) = Just x

atomicallyR, liftSTM :: MonadIO m => STM a -> m a
-- | Convenience function to lift computations in @MonadIO@ monads into the STM monad
liftSTM = liftIO.atomically
-- | Convenience function to perform STM computations in @MonadIO@ instances
atomicallyR = liftSTM

-- | Handles the special ".." and "." path elements to produce a normalized path
normalizedPath :: [Text] -> [Text]
normalizedPath (x:"..":xs) = normalizedPath xs
normalizedPath (".":xs) = normalizedPath xs
normalizedPath (x:xs) = x : normalizedPath xs
normalizedPath [] = []

-- | Takes in a `Network.Wai.Request' object and spits out an `Either ByteString StdMethod' that is
-- @Left e@ if an error occurred or @Right method@ if not
method = parseMethod.requestMethod

-- | Converts a `Source' of `ByteString' to a single `ByteString'
sourceToBS :: Monad m => Source m BS.ByteString -> m BS.ByteString
sourceToBS s = s $$ myDecode (fromString "")
    where
      myDecode a = do
        res <- await
        case res of
          Nothing -> return a
          Just x -> myDecode (BS.append a x)

-- | Convenience function to raise error if JSON decoding method returns `Error'
failOnError :: Result a -> (a -> b) -> b
failOnError (Error e) _ = error e
failOnError (Ok a) f = f a

-- diffLocalTime :: LocalTime -> LocalTime -> DiffTime
-- diffLocalTime a b = let daysBetween = (localDay a) `diffDays` (localDay b)
--                         timeDiffSeconds = (timeOfDayToTime $ localTimeOfDay a) - (timeOfDayToTime $ localTimeOfDay b)
--                     in (fromIntegral daysBetween) * 86400 + timeDiffSeconds