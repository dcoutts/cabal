module Distribution.Client.FileStatusCache where

import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.ByteString.Lazy as BS
import           Data.Binary
import qualified Data.Binary as Binary
import           Data.Hashable
import           Data.Time (UTCTime(..), Day(..))
import           Control.Applicative
import           Control.Exception

import           System.FilePath
import           System.Directory
import           System.IO
import           System.IO.Error


-- contains a mapping from normalised relative paths to timestamp and content hash.
newtype FileStatusCache = FileStatusCache (Map FilePath FileInfo)

data FileInfo = FileInfo {-# UNPACK #-} !UTCTime
                         {-# UNPACK #-} !Hash

type Hash = Int

emptyFileStatusCache :: FileStatusCache
emptyFileStatusCache = FileStatusCache Map.empty

-- | Given a 'FileStatusCache' and a set of files that we are interested in,
-- check if any of those files have changed. The set of files and the cache
-- are both taken to be relative to the given root directory.
--
-- A change here includes additions or deletions compared to the set of files
-- that we are interested in, and any change in content.
--
-- To make this a bit faster it relies on an assumption: files where the
-- modification timestamps have not chagnged are assumed to have not changed
-- content. On the other hand, if there is change in modification timestamp, a
-- file content hash is checked to see if it's really changed. So in the
-- typical case only file timestamps need to be checked.
--
checkFileStatusChanged :: FilePath -> FilePath -> IO Bool
checkFileStatusChanged dir statusCacheFile = do
    mfileCache <- handleDoesNotExist (\_ -> return Nothing) $
                    either (\_ -> return Nothing)
                           (return . Just)
                       =<< Binary.decodeFileOrFail statusCacheFile
    case mfileCache of
      Nothing -> return True
      Just (FileStatusCache fileCache) ->
        -- Assume the files we are interested in are all those in the cache.
        -- So, we go and probe them all.
        probe (Map.toList fileCache)
  where
    probe []                                      = return False
    probe ((relfile, FileInfo mtime chash):files) = do
      let file = dir </> relfile
      mtime' <- getModificationTime file
      if mtime == mtime'
        then probe files
        -- Only read the file and calculate the hash if the mtime changed
        else do unchanged <-
                  handleDoesNotExist (\_ -> return True) $ do
                    chash' <- readFileHash file
                    if chash == chash'
                      then return ()
                      else print ("checkFileStatusChanged", file, chash, chash')
                    return (chash == chash')
                if unchanged
                  then probe files
                  else return True

{-
checkFileStatusChanged :: FilePath -> FileStatusCache -> Set FilePath -> IO Bool
checkFileStatusChanged dir (FileStatusCache fileCache) knownFiles

    -- First check if the cache mentions all the files and only the files we
    -- are interested in. If so, we go on to probe them all.
    | Map.keysSet fileCache == knownFiles
    = probe (Map.toList (fileCache `Map.intersection` knownFiles'))

    -- If not, we can bail out immediately as something has changed.
    | otherwise
    = return True
  where
    knownFiles' = Map.fromSet (const ()) knownFiles

    probe []                                      = return False
    probe ((relfile, FileInfo mtime chash):files) = do
      let file = dir </> relfile
      mtime' <- getModificationTime file
      if mtime == mtime'
        then probe files
        else do
          -- Only read the file and calculate the hash if the mtime changed
          chash' <- readFileHash file
          if chash == chash'
            then probe files
            else return True
-}


updateFileStatusCache :: FilePath -> FilePath -> [FilePath] -> IO ()
updateFileStatusCache dir statusCacheFile knownFiles = do
    FileStatusCache fileCacheOld <-
      handleDoesNotExist (\_ -> return emptyFileStatusCache) $
        either (\_ -> return emptyFileStatusCache)
               return
           =<< Binary.decodeFileOrFail statusCacheFile

    -- Go over each of the knownFiles, joined with the old cache
    -- and return up to date file info, reusing the cache where possible
    fileCache <- Map.traverseWithKey probe $
                   leftJoinWith (\_ -> Nothing) (\_ finfo -> Just finfo)
                                (Map.fromList (map (\k->(k,())) knownFiles))
                                fileCacheOld

    Binary.encodeFile statusCacheFile (FileStatusCache fileCache)
  where
    probe :: FilePath -> Maybe FileInfo -> IO FileInfo
    probe relfile Nothing = do
      -- new file, have to get file info
      let file = dir </> relfile
      readFileInfo file

    probe relfile (Just finfo@(FileInfo mtime _chash)) = do
      -- existing file, check file info and update if necessary
      let file = dir </> relfile
      mtime' <- getModificationTime file
      if mtime == mtime'
        then return finfo
        else readFileInfo file

leftJoinWith :: Ord k => (a -> c) -> (a -> b -> c)
             -> Map k a -> Map k b -> Map k c
leftJoinWith left middle =
    Map.mergeWithKey (\_ a b -> Just $! middle a b)  -- join the inner
                     (Map.map left)                  -- include the left outer
                     (const Map.empty)               -- drop the right

readFileHash :: FilePath -> IO Hash
readFileHash file =
    withBinaryFile file ReadMode $ \hnd ->
      evaluate . hash =<< BS.hGetContents hnd
    
readFileInfo :: FilePath -> IO FileInfo
readFileInfo file =
    FileInfo <$> getModificationTime file
             <*> readFileHash file

handleDoesNotExist :: (IOError -> IO a) -> IO a -> IO a
handleDoesNotExist =
    handleJust (\ioe -> if isDoesNotExistError ioe then Just ioe else Nothing)

instance Binary FileInfo where
  put (FileInfo a b) = do put a >> put b
  get = do a <- get; b <- get; return $! FileInfo a b

instance Binary UTCTime where
  put (UTCTime (ModifiedJulianDay day) tod) = do
    put day
    put (toRational tod)
  get = do
    day  <- get
    tod <- get
    return $! UTCTime (ModifiedJulianDay day)
                      (fromRational tod)

instance Binary FileStatusCache where
  put (FileStatusCache fileCache) = do
    put (0 :: Int) -- version
    put fileCache
  get = do
    ver <- get
    if ver == (0 :: Int)
      then do fileCache <- get
              return $! FileStatusCache fileCache
      -- note, it's ok if the format is wrong to just return empty
      else return emptyFileStatusCache

---------------------------------------------------------------------

checkValueChanged :: (Binary a, Eq a) => FilePath -> a -> IO Bool
checkValueChanged cacheFile currentValue =
    handleDoesNotExist (\_ -> return True) $ do   -- cache file didn't exist
      res <- Binary.decodeFileOrFail cacheFile
      case res of          
        Right cachedValue
          | currentValue == cachedValue
                       -> return False
          | otherwise  -> return True -- value changed
        Left _         -> return True -- decode error


updateValueChangeCache :: Binary a => FilePath -> a -> IO ()
updateValueChangeCache = Binary.encodeFile

