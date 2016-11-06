package org.apache.spark.deploy

import java.io.File

import org.apache.spark.deploy.worker.Worker
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.extension.KernelExtensionFactory
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.impl.spi.KernelContext
import org.neo4j.kernel.impl.util.JobScheduler
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.{Lifecycle, LifecycleAdapter}
import org.neo4j.spark.Neo4j
import org.neo4j.kernel.AvailabilityGuard
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard

/**
  * @author mh
  * @since 13.10.16
  */
object SparkExtensionFactory {

  private[spark] trait Dependencies {
    def graphdatabaseAPI: GraphDatabaseAPI

    def scheduler: JobScheduler

    def procedures: Procedures

    def config: Config

//    def guard : CoreAPIAvailabilityGuard
  }

}

class SparkExtensionFactory protected() extends KernelExtensionFactory[SparkExtensionFactory.Dependencies]("spark-on-neo4j") {

  @throws[Throwable]
  def newInstance(kernelContext: KernelContext, dependencies: SparkExtensionFactory.Dependencies): Lifecycle = {

    System.setProperty("spark.testing","true")
    val sparkHome: String = new File(dependencies.graphdatabaseAPI.getStoreDir, "spark").getAbsolutePath
    System.setProperty("spark.test.home",sparkHome)
    System.setProperty("SPARK_SCALA_VERSION","2.11")
    val sparkMasterUrl: String = dependencies.config.getParams.getOrDefault("spark.master", "spark://localhost:7077")

    new LifecycleAdapter() {

      @throws[Throwable]
      override def start() {
        // todo inject "Neo4j" instance into SparkContext
        Neo4j.db = dependencies.graphdatabaseAPI
//        val guard = dependencies.guard
        new Thread() {
          override def run() {
//            while (!guard.isAvailable(5000)) {
//              if (guard.isShutdown ) return
              Thread.sleep(5000)
//            }
            Worker.main(Array[String](sparkMasterUrl))
          }
        }.start()
      }
    }
  }
}
