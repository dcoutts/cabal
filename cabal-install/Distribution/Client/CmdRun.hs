{-# LANGUAGE NamedFieldPuns #-}

-- | cabal-install CLI command: run
--
module Distribution.Client.CmdRun (
    runCommand,
    runAction,
  ) where

import Distribution.Client.ProjectOrchestration
import Distribution.Client.ProjectConfig
         ( BuildTimeSettings(..) )
import Distribution.Client.BuildTarget
         ( readUserBuildTargets )

import Distribution.Client.Setup
         ( GlobalFlags, ConfigFlags(..), ConfigExFlags, InstallFlags )
import qualified Distribution.Client.Setup as Client
import Distribution.Simple.Setup
         ( HaddockFlags, fromFlagOrDefault )
import Distribution.Simple.Command
         ( CommandUI(..), usageAlternatives )
import Distribution.Verbosity
         ( normal )
import Distribution.Simple.Utils
         ( wrapText, die )

import qualified Data.Map as Map
import Control.Monad (when)


runCommand :: CommandUI (ConfigFlags, ConfigExFlags, InstallFlags, HaddockFlags)
runCommand = Client.installCommand {
  commandName         = "new-run",
  commandSynopsis     = "Run an executable.",
  commandUsage        = usageAlternatives "new-run"
                          [ "[TARGET] [FLAGS] [-- EXECUTABLE_FLAGS]" ],
  commandDescription  = Just $ \pname -> wrapText $
        "Runs the specified executable, first ensuring it is up to date.\n\n"

     ++ "Any executable in any package in the project can be specified. "
     ++ "A package can be specified if contains just one executable. "
     ++ "The default is to use the package in the current directory if it "
     ++ "contains just one executable.\n\n"

     ++ "Extra arguments can be passed to the program, but use '--' to "
     ++ "separate arguments for the program from arguments for " ++ pname
     ++ ". The executable is run in an environment where it can find its "
     ++ "data files inplace in the build tree.\n\n"

     ++ "Dependencies are built or rebuilt as necessary. Additional "
     ++ "configuration flags can be specified on the command line and these "
     ++ "extend the project configuration from the 'cabal.project', "
     ++ "'cabal.project.local' and other files.",
  commandNotes        = Just $ \pname ->
        "Examples:\n"
     ++ "  " ++ pname ++ " new-run\n"
     ++ "    Run the executable in the package in the current directory\n"
     ++ "  " ++ pname ++ " new-run foo-tool\n"
     ++ "    Run the named executable (in any package in the project)\n"
     ++ "  " ++ pname ++ " new-run pkgfoo:foo-tool\n"
     ++ "    Run the executable 'foo-tool' in the package 'pkgfoo'\n"
     ++ "  " ++ pname ++ " new-run foo -O2 -- dothing --fooflag\n"
     ++ "    Build with '-O2' and run the program, passing it extra arguments.\n\n"

     ++ "Note: this command is part of the new project-based system (aka "
     ++ "nix-style\nlocal builds). These features are currently in beta. "
     ++ "Please see\n"
     ++ "http://cabal.readthedocs.io/en/latest/nix-local-build-overview.html "
     ++ "for\ndetails and advice on what you can expect to work. If you "
     ++ "encounter problems\nplease file issues at "
     ++ "https://github.com/haskell/cabal/issues and if you\nhave any time "
     ++ "to get involved and help with testing, fixing bugs etc then\nthat "
     ++ "is very much appreciated.\n"
   }

-- | The @build@ command does a lot. It brings the install plan up to date,
-- selects that part of the plan needed by the given or implicit targets and
-- then executes the plan.
--
-- For more details on how this works, see the module
-- "Distribution.Client.ProjectOrchestration"
--
runAction :: (ConfigFlags, ConfigExFlags, InstallFlags, HaddockFlags)
          -> [String] -> GlobalFlags -> IO ()
runAction (configFlags, configExFlags, installFlags, haddockFlags)
            targetStrings globalFlags = do

    userTargets <- readUserBuildTargets targetStrings

    buildCtx <-
      runProjectPreBuildPhase
        verbosity
        ( globalFlags, configFlags, configExFlags
        , installFlags, haddockFlags )
        PreBuildHooks {
          hookPrePlanning      = \_ _ _ -> return (),

          hookSelectPlanSubset = \buildSettings
                                  elaboratedPlan localPackages -> do
            when (buildSettingOnlyDeps buildSettings) $
              die $ "The repl command does not support '--only-dependencies'. "
                 ++ "You may wish to use 'build --only-dependencies' and then "
                 ++ "use 'repl'."

            -- Interpret the targets on the command line as build targets
            -- (as opposed to say repl or haddock targets).
            targets <- either reportRunTargetProblems return
                   =<< resolveTargets
                         selectPackageTargets
                         selectComponentTarget
                         TargetProblemCommon
                         elaboratedPlan
                         localPackages
                         userTargets

            when (Map.size targets > 1) $
              let problem = TargetsMultiple (Map.elems targets)
               in reportRunTargetProblems [problem]

            --TODO: [required eventually] handle no targets case
            when (Map.null targets) $
              fail "TODO handle no targets case"

            let elaboratedPlan' = pruneInstallPlanToTargets
                                    TargetActionBuild
                                    targets
                                    elaboratedPlan
            return (elaboratedPlan', targets)
        }

    printPlan verbosity buildCtx

    buildOutcomes <- runProjectBuildPhase verbosity buildCtx
    runProjectPostBuildPhase verbosity buildCtx buildOutcomes
  where
    verbosity = fromFlagOrDefault normal (configVerbosity configFlags)

-- For run: select the exe if there is only one and it's buildable.
-- Fail if there are no or multiple buildable exe components.
--
selectPackageTargets :: BuildTarget PackageId
                     -> [AvailableTarget k] -> Either RunTargetProblem [k]
selectPackageTargets _bt ts
  | [exet] <- exets    = Right [exet]
  | (_:_)  <- exets    = Left TargetPackageMultipleExes

  | (_:_)  <- allexets = Left TargetPackageNoBuildableExes
  | otherwise          = Left TargetPackageNoTargets
  where
    allexets = [ t | t <- ts, availableTargetIsExe t ]
    exets    = [ k | TargetBuildable k _ <- map availableTargetStatus allexets ]

selectComponentTarget :: BuildTarget PackageId
                      -> AvailableTarget k -> Either RunTargetProblem  k
selectComponentTarget bt t
  | not (availableTargetIsTest t)
              = Left (TargetComponentNotExe (fmap (const ()) t))
  | otherwise = either (Left . TargetProblemCommon) return $
                selectComponentTargetBasic bt t

data RunTargetProblem =
     TargetPackageMultipleExes
   | TargetPackageNoBuildableExes
   | TargetPackageNoTargets
   | TargetComponentNotExe (AvailableTarget ())
   | TargetProblemCommon    TargetProblem
   | TargetsMultiple [[ComponentTarget]] --TODO: more detail needed
  deriving Show

reportRunTargetProblems :: [RunTargetProblem] -> IO a
reportRunTargetProblems = fail . show

{-

runAction :: (BuildFlags, BuildExFlags) -> [String] -> Action
runAction (buildFlags, buildExFlags) extraArgs globalFlags = do
  let verbosity   = fromFlagOrDefault normal (buildVerbosity buildFlags)
  (useSandbox, config) <- loadConfigOrSandboxConfig verbosity globalFlags
  distPref <- findSavedDistPref config (buildDistPref buildFlags)
  let noAddSource = fromFlagOrDefault DontSkipAddSourceDepsCheck
                    (buildOnly buildExFlags)

  -- reconfigure also checks if we're in a sandbox and reinstalls add-source
  -- deps if needed.
  config' <-
    reconfigure configureAction
    verbosity distPref useSandbox noAddSource (buildNumJobs buildFlags)
    mempty [] globalFlags config

  lbi <- getPersistBuildConfig distPref
  (exe, exeArgs) <- splitRunArgs verbosity lbi extraArgs

  maybeWithSandboxDirOnSearchPath useSandbox $
    build verbosity config' distPref buildFlags ["exe:" ++ exeName exe]

  maybeWithSandboxDirOnSearchPath useSandbox $
    run verbosity lbi exe exeArgs


-- | Return the executable to run and any extra arguments that should be
-- forwarded to it. Die in case of error.
splitRunArgs :: Verbosity -> LocalBuildInfo -> [String]
             -> IO (Executable, [String])
splitRunArgs verbosity lbi args =
  case whichExecutable of -- Either err (wasManuallyChosen, exe, paramsRest)
    Left err               -> do
      warn verbosity `traverse_` maybeWarning -- If there is a warning, print it.
      die err
    Right (True, exe, xs)  -> return (exe, xs)
    Right (False, exe, xs) -> do
      let addition = " Interpreting all parameters to `run` as a parameter to"
                     ++ " the default executable."
      -- If there is a warning, print it together with the addition.
      warn verbosity `traverse_` fmap (++addition) maybeWarning
      return (exe, xs)
  where
    pkg_descr = localPkgDescr lbi
    whichExecutable :: Either String       -- Error string.
                              ( Bool       -- If it was manually chosen.
                              , Executable -- The executable.
                              , [String]   -- The remaining parameters.
                              )
    whichExecutable = case (enabledExes, args) of
      ([]   , _)           -> Left "Couldn't find any enabled executables."
      ([exe], [])          -> return (False, exe, [])
      ([exe], (x:xs))
        | x == exeName exe -> return (True, exe, xs)
        | otherwise        -> return (False, exe, args)
      (_    , [])          -> Left
        $ "This package contains multiple executables. "
        ++ "You must pass the executable name as the first argument "
        ++ "to 'cabal run'."
      (_    , (x:xs))      ->
        case find (\exe -> exeName exe == x) enabledExes of
          Nothing  -> Left $ "No executable named '" ++ x ++ "'."
          Just exe -> return (True, exe, xs)
      where
        enabledExes = filter (buildable . buildInfo) (executables pkg_descr)

    maybeWarning :: Maybe String
    maybeWarning = case args of
      []    -> Nothing
      (x:_) -> lookup x components
      where
        components :: [(String, String)] -- Component name, message.
        components =
          [ (name, "The executable '" ++ name ++ "' is disabled.")
          | e <- executables pkg_descr
          , not . buildable . buildInfo $ e, let name = exeName e]

          ++ [ (name, "There is a test-suite '" ++ name ++ "',"
                      ++ " but the `run` command is only for executables.")
             | t <- testSuites pkg_descr
             , let name = testName t]

          ++ [ (name, "There is a benchmark '" ++ name ++ "',"
                      ++ " but the `run` command is only for executables.")
             | b <- benchmarks pkg_descr
             , let name = benchmarkName b]

-- | Run a given executable.
run :: Verbosity -> LocalBuildInfo -> Executable -> [String] -> IO ()
run verbosity lbi exe exeArgs = do
  curDir <- getCurrentDirectory
  let buildPref     = buildDir lbi
      pkg_descr     = localPkgDescr lbi
      dataDirEnvVar = (pkgPathEnvVar pkg_descr "datadir",
                       curDir </> dataDir pkg_descr)

  (path, runArgs) <-
    case compilerFlavor (compiler lbi) of
      GHCJS -> do
        let (script, cmd, cmdArgs) =
              GHCJS.runCmd (withPrograms lbi)
                           (buildPref </> exeName exe </> exeName exe)
        script' <- tryCanonicalizePath script
        return (cmd, cmdArgs ++ [script'])
      _     -> do
         p <- tryCanonicalizePath $
            buildPref </> exeName exe </> (exeName exe <.> exeExtension)
         return (p, [])

  env  <- (dataDirEnvVar:) <$> getEnvironment
  -- Add (DY)LD_LIBRARY_PATH if needed
  env' <- if withDynExe lbi
             then do let (Platform _ os) = hostPlatform lbi
                     clbi <- case componentNameTargets' pkg_descr lbi (CExeName (exeName exe)) of
                                [target] -> return (targetCLBI target)
                                [] -> die "run: Could not find executable in LocalBuildInfo"
                                _ -> die "run: Found multiple matching exes in LocalBuildInfo"
                     paths <- depLibraryPaths True False lbi clbi
                     return (addLibraryPath os paths env)
             else return env
  notice verbosity $ "Running " ++ exeName exe ++ "..."
  rawSystemExitWithEnv verbosity path (runArgs++exeArgs) env'

-}
