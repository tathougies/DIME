module Language.Flow.Util where

mapInit :: (a -> a) -> [a] -> [a]
mapInit _ (x:[]) = [x]
mapInit f (x:xs) = (f x):(mapInit f xs)