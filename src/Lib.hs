{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE TemplateHaskell #-}
--{-# CPP #-}

-- | use-haskell
-- The purpose of this project is to provide a baseline demonstration of the use of cloudhaskell in the context of the
-- code complexity measurement individual programming project. The cloud haskell platform provides an elegant set of
-- features that support the construction of a wide variety of multi-node distributed systems commuinication
-- architectures. A simple message passing abstraction forms the basis of all communication.
--
-- This project provides a command line switch for starting the application in master or worker mode. It is implemented
-- using the work-pushing pattern described in http://www.well-typed.com/blog/71/. Comments below describe how it
-- operates. A docker-compose.yml file is provided that supports the launching of a master and set of workers.

module Lib
    ( someFunc
    ) where

-- These imports are required for Cloud Haskell
import           Control.Distributed.Process
import           Control.Distributed.Process.Backend.SimpleLocalnet
import           Control.Distributed.Process.Closure
import           Control.Distributed.Process.Node                   (initRemoteTable)
import           Control.Monad
import           Network.Transport.TCP                              (createTransport,
                                                                     defaultTCPParameters)
import           PrimeFactors
import           System.Environment                                 (getArgs)
import           System.Exit

-- | worker function.
-- This is the function that is called to launch a worker. It loops forever, asking for work, reading its message queue
-- and sending the result of runnning numPrimeFactors on the message content (an integer).
worker :: (ProcessId, NodeId, String) -> Process ()
worker (master, workerId, url) = do
  liftIO ( putStrLn $ "Worker : " ++ (show workerId) ++ " started with parameter: " ++ url) 
  let repoName = last $ splitOn "/" url
  gitRepoExists <- liftIO $ doesDirectoryExist ("/test/" ++ repoName)
  if not gitRepoExists then do
    liftIO $ callProcess "/usr/bin/git" ["clone", url, "/test/" ++ repoName]
  else do
    liftIO $ putStrLn "Repository exists!"
  let conf = (Config 6 [] [] [] Colored)
  let source = allFiles ("/test/" ++ repoName)
              >-> P.mapM (liftIO . analyze conf)
              >-> P.map (filterResults conf)
              >-> P.filter filterNulls
  liftIO $ putStrLn $ "Analyse Started for" ++ url
  (output, _) <- liftIO $ capture $ runSafeT $ runEffect $ exportStream conf source
  liftIO ( putStrLn $ "Worker : " ++ (show workerId) ++ " finished work with parameter: " ++ url)
  send master $ (workerId, url, output)

remotable ['worker]

myRemoteTable :: RemoteTable
myRemoteTable = Main.__remoteTable initRemoteTable

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["master", host, port] -> do
      start <- getCurrentTime
      backend <- initializeBackend host port myRemoteTable
      startMaster backend (master backend)
      end <- getCurrentTime
      let timetaken = diffUTCTime end start
      appendFile "record.txt" (show timetaken ++ ",")
    ["slave", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startSlave backend

