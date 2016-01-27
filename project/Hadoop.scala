 // in the name of ALLAH

import sbt._
import Keys._
import sbt.complete._
import sbt.complete.DefaultParsers._

import scala.util.{Try, Success=>StdSuccess, Failure=>StdFailure}
import scala.collection.mutable
import java.io.File

object HadoopPlugin extends AutoPlugin {

  type Dict = Map[String,String] // Keys.Classpath

  def mainClassParser:Parser[String] = 
    Space ~ token(any.* map(_.mkString)) map { _._2 }

  object forAll {
    val hadoop = config("hadoop") describedAs "Hadoop"
    val hadoopHomeDir = settingKey[Option[File]]("Hadoop's home directory")
    val hadoopConfDir = settingKey[Option[File]]("Hadoop's conf directory")
    val hadoopEnv = settingKey[Dict]("Hadoop's enviroment options")
    val hadoopClasspath = taskKey[Classpath]("Hadoop's classpath")
  }
  
  object forSelf {
    val pluginConf = taskKey[Config]("Hadoop Plugin's configuration")
    val getInfo = taskKey[Dict]("Hadoop's version, home, etc.")
    val info = taskKey[Unit]("Print Hadoop's version, home, etc.")
    val runJob = inputKey[Unit]("Run Hadoop's job main-class")
  }

  object specialTasks {
    val formatNameNode = taskKey[Unit]("Format name node")
    val startDFS = taskKey[Unit]("Start DFS")
    val stopDFS = taskKey[Unit]("Stop DFS")
    val startYARN = taskKey[Unit]("Start YRAN")
    val stopYARN = taskKey[Unit]("Stop Yran")
  }
  
  val autoImport = forAll

  import forAll._
  import forSelf._
  import specialTasks._

  override def projectConfigurations = Seq(hadoop)

  override def requires = plugins.CorePlugin && 
  plugins.JvmPlugin && 
  plugins.IvyPlugin

  override def trigger = allRequirements

  val publishedAPI = inConfig(hadoop){Seq(
    hadoopHomeDir := None, 
    hadoopConfDir := None,
    javaHome := javaHome.value,
    pluginConf := {
      val logger = streams.value.log
      val homedir = (hadoopHomeDir in hadoop).value
      val confdir = (hadoopConfDir in hadoop).value
    
      if (homedir.isEmpty) error("Set 'homeDir in hadoop'", logger)
      if (confdir.isEmpty) error("Set 'confDir in hadoop'", logger)

      Config(homedir.get, confdir.get, logger)
    },
    hadoopClasspath := {
      implicit val conf = pluginConf.value
      classpathSeq.classpath
    },
    getInfo := {
      implicit val conf = pluginConf.value
      val info = Map(
        "home" -> bin.getAbsolutePath,
        "bin" -> bin.getAbsolutePath,
        "conf" -> conf.confDir.getAbsolutePath,
        "version" -> version,
        "classpath" -> classpathString
      )
      info
    },
    info := {
      implicit val conf = pluginConf.value
      val info = getInfo.value 
      info foreach {
        case (key,value) =>
          conf.logger info s"${key}:\t${value}"
      }
    },
    runJob := {
      
      implicit val conf = pluginConf.value
      val packaged = (packageBin in Compile).value
      val mainClass = mainClassParser.parsed
      val classpath = (hadoopClasspath in hadoop).value

      // All of them should be "*.jar"
      val allClasspath = (externalDependencyClasspath in Runtime).value.map(_.data)
      val depJar = allClasspath.
        // filter { i => i.isFile && i.getName.endsWith(".jar") }.
        map { _.getAbsolutePath }

      conf.logger info mainClass
      conf.logger info "Tunrime Deps:"
      allClasspath foreach { i => conf.logger info i.getAbsolutePath }

      conf.logger info depJar.mkString(",")

      val cmd = s"""${conf.homeDir.getAbsolutePath}/bin/hadoop
                   | --config ${conf.confDir.getAbsolutePath}
                   | jar ${packaged}
                   | ${mainClass}
                   | -libjars ${depJar.mkString(",")}
                   | """.stripMargin.replace("\n","")
      val env = Map("HADOOP_CLASSPATH" -> depJar.mkString(File.pathSeparator))
      println(cmd)
      println(env)

      process(cmd, env).get
    },
    formatNameNode := {
      implicit val conf = pluginConf.value
      val confDir = conf.confDir.getAbsolutePath
      val hdfsBin = conf.homeDir / "bin" / "hdfs" getAbsolutePath
      val cmd = s"${hdfsBin} --config ${confDir} namenode -format"
      Process(cmd).run(new StdLogger(conf.logger))
    },
    startDFS := runSbin("start-dfs.sh")(pluginConf.value),
    stopDFS := runSbin("stop-dfs.sh")(pluginConf.value),
    startYARN := runSbin("start-yran.sh")(pluginConf.value),
    stopYARN := runSbin("stop-yarn.sh")(pluginConf.value)
  )}

  private val hadoopPluginConf = (pluginConf in hadoop)

  override def projectSettings = publishedAPI ++ Seq(
    /*
    overrideClasspath(unmanagedClasspath, Runtime, hadoopPluginConf),
    overrideClasspath(unmanagedClasspath, Compile, hadoopPluginConf),
    overrideClasspath(unmanagedClasspath, Test, hadoopPluginConf)
    */
    allDependencies := {
      implicit val conf = (pluginConf in hadoop).value
      val info = (getInfo in hadoop).value
      val version = info("version")
      val id = ModuleID(
        "org.apache.hadoop",
        "hadoop-client",
        version,
        Some("provided")
      )
      allDependencies.value ++ Seq(id)
    }
  )

  // =====

  case class Config(
    val homeDir: File,
    val confDir: File,
    val logger: Logger
  )

  class StdLogger(logger:Logger) extends ProcessLogger {
    def info(s: => String): Unit = logger info s
    def error(s: => String):Unit = logger error s
    def buffer[T](f: => T): T = f
  }

  protected def error(str: String, logger: Logger):Unit = {
    logger error str
    throw new Exception(str)
  }

  protected def error(str: String)(implicit conf:Config):Unit =
    error(str, conf.logger)



  protected def getBin(home:File):File = home / "bin" / "hadoop"

  protected def bin(implicit conf: Config) = getBin(conf.homeDir)

  protected def getVersion(bin:File):String = 
    Process(s"${bin.getAbsolutePath} version").lines.head.
      replace("Hadoop ","")

  protected def version(implicit conf:Config) = getVersion(bin)

  protected def getClasspathString(bin:File):String = 
    Process(s"${bin.getAbsolutePath} classpath").lines.head

  protected def classpathString(implicit conf: Config) = getClasspathString(bin)

  protected def classpathSeq(implicit conf: Config):Seq[File] = 
    (classpathString.split(File.pathSeparator) map file).flatMap { f =>
      if (f.exists) Seq(f)
      else if (f.getName endsWith "*") 
        (file(f.getParent) * "*").get.toList
      else if (f.getName endsWith "jar") 
        (file(f.getParent) * "*.jar").get.toList
      else {
        conf.logger warn "I'm here :("
        Seq()
      }
    }

  protected def overrideClasspath(
    task:TaskKey[Classpath], 
    scope:sbt.Configuration,
    conf: Config
  ) = (task in scope) := {
    /*val paths = (hadoopClasspath in hadoop).value
    conf.logger debug s"Overriding classpaths for task(${task}) in scope(${scope}) to injecting hadoop's classpaths"
    (task in scope).value ++ paths.classpath*/
    (task in scope).value ++ (hadoopClasspath in hadoop).value
  }

  protected def process(
    cmd: String, 
    env: Map[String,String] = Map.empty[String,String]
  )(
    implicit conf: Config
  ):Try[Unit] = {
    val stdlogger = new StdLogger(conf.logger)
    val proc = Process(cmd, None, env.toSeq:_*) run stdlogger
    if (proc.exitValue == 0) StdSuccess(())
    else {
      val msg = s"Job failed (exit-code: ${proc.exitValue})"
      conf.logger error msg
      StdFailure(new Exception(msg))
    }
  }

  protected def runSbin(sbin: String)(implicit conf: Config) = {
    val confDir = conf.confDir.getAbsolutePath
    val bin = conf.homeDir / "sbin" / sbin  getAbsolutePath
    val cmd = s"${bin} --config ${confDir}"
    conf.logger debug cmd
    process(cmd).get
  }
}
