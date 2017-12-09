/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, AllTuples, UnspecifiedDistribution}
import org.apache.spark.util.collection.SizeTrackingAppendOnlyMap

case class SpillableAggregate(
                               partial: Boolean,
                               groupingExpressions: Seq[Expression],
                               aggregateExpressions: Seq[NamedExpression],
                               child: SparkPlan) extends UnaryNode {

  override def requiredChildDistribution =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def output = aggregateExpressions.map(_.toAttribute)

  /**
    * An aggregate that needs to be computed for each row in a group.
    *
    * @param unbound Unbound version of this aggregate, used for result substitution.
    * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
    * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
    *                        output.
    */
  case class ComputedAggregate(
                                unbound: AggregateExpression,
                                aggregate: AggregateExpression,
                                resultAttribute: AttributeReference)

  /** Physical aggregator generated from a logical expression.  */
  private[this] val computedAggregates = aggregateExpressions.flatMap { agg => // list of letter
    agg.collect {
      case a: AggregateExpression =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, child.output),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  private[this] val aggregator: ComputedAggregate = computedAggregates(0)
  /** Schema of the aggregate.  */
  private[this] val aggregatorSchema: AttributeReference = aggregator.resultAttribute // IMPLEMENT ME

  /** Creates a new aggregator instance.  */
  // one buffer/Instance for only one aggregator
  private[this] def newAggregatorInstance(): AggregateFunction = aggregator.aggregate.newInstance() // IMPLEMENT ME

  /** Named attributes used to substitute grouping attributes in the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
    * A map of substitutions that are used to insert the aggregate expressions and grouping
    * expression into the final result expression.
    */
  protected val resultMap =
    ( Seq(aggregator.unbound -> aggregator.resultAttribute) ++ namedGroups).toMap

  /**
    * Substituted version of aggregateExpressions expressions which are used to compute final
    * output rows given a group and the result of all aggregate computations.
    */
  private[this] val resultExpression = aggregateExpressions.map(agg => agg.transform {
    case e: Expression if resultMap.contains(e) => resultMap(e)
  }
  )

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions(iter => generateIterator(iter))
  }

  /**
    * This method takes an iterator as an input. The iterator is drained either by aggregating
    * values or by spilling to disk. Spilled partitions are successively aggregated one by one
    * until no data is left.
    *
    * @param input the input iterator
    * @param memorySize the memory size limit for this aggregate
    * @return the result of applying the projection
    */
  def generateIterator(input: Iterator[Row], memorySize: Long = 64 * 1024 * 1024, numPartitions: Int = 64): Iterator[Row] = {
    val groupingProjection = CS143Utils.getNewProjection(groupingExpressions, child.output)
    var currentAggregationTable = new SizeTrackingAppendOnlyMap[Row, AggregateFunction]
    var data = input

    // return the spilled partitions array
    def initSpills(): Array[DiskPartition] = {
      /* IMPLEMENT THIS METHOD */
      val Spills: Array[DiskPartition] = new Array[DiskPartition](numPartitions)
      for (i <- 0 to (numPartitions - 1)) {
        //  set blockSize to 0, otherwise spilled records will stay in memory and not actually spill to disk!
        //  by assumption ?
        val blockSize = 0
        Spills(i) = new DiskPartition("spillPartition" ++ i.toString, blockSize)
      }
      Spills
    }

    val spills = initSpills()

    new Iterator[Row] {
      /**
        * Global object wrapping the spills into a DiskHashedRelation. This variable is
        * set only when we are sure that the input iterator has been completely drained.
        *
        * @return
        */
      var hashedSpills: Option[Iterator[DiskPartition]] = None
      var diskHashedRelation: Option[DiskHashedRelation] = None
      var aggregateResult: Iterator[Row] = aggregate()
      val diskPartitionIterator = spills.iterator

      def hasNext() = {
        if(aggregateResult.hasNext){
          true
        }
        else fetchSpill()
//        val result = aggregateResult.hasNext || (diskPartitionIterator.hasNext && fetchSpill())
//        if (!result) {
          // close each DiskPartition
          // remove partition temporary files
//          for (thispartition <- spills) {
//            thispartition.closePartition()
//          }
//        }
//        result
      }

      def next() = {
        /* IMPLEMENT THIS METHOD */
//        if (!aggregateResult.hasNext ) {
//          fetchSpill()
//        }
        if(!aggregateResult.hasNext && !fetchSpill()){
          throw new NoSuchElementException("no data")
        }
        else{
          if(!aggregateResult.hasNext){
            fetchSpill()
          }
          aggregateResult.next()
        }
//        aggregateResult.next()
      }

      /**
        * This method load the aggregation hash table by draining the data iterator
        *
        * @return
        */
      private def aggregate(): Iterator[Row] = {
        /* IMPLEMENT THIS METHOD */
        //        child.execute().mapPartitions { iter =>
        //          val hashTable = new HashMap[Row, Array[AggregateFunction]]
        //          val groupingProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)
        //
        //          var currentRow: Row = null
        //          while (iter.hasNext) {
        //            currentRow = iter.next()
        //            val currentGroup = groupingProjection(currentRow)
        //            var currentBuffer = hashTable.get(currentGroup)
        //            if (currentBuffer == null) {
        //              currentBuffer = newAggregateBuffer()
        //              hashTable.put(currentGroup.copy(), currentBuffer)
        //            }
        //
        //            var i = 0
        //            while (i < currentBuffer.length) {
        //              currentBuffer(i).update(currentRow)
        //              i += 1
        //            }
        var curRow: Row = null
        while (data.hasNext) {
          curRow = data.next()
          val curGroup = groupingProjection(curRow)
          // work as hashmap
          var curBuffer = currentAggregationTable(curGroup)


          // not in hashTable
          if (curBuffer == null) {
            // check the size of AggregationTable
            curBuffer = newAggregatorInstance()
            if (CS143Utils.maybeSpill(currentAggregationTable, memorySize)) {
              spillRecord(curGroup, curRow)
            } else {

              currentAggregationTable.update(curGroup, curBuffer)
              curBuffer.update(curRow)
            }
          } else curBuffer.update(curRow)


           curBuffer.update(curRow)
        }
        if(data==input){
          for (i <- 0 to (numPartitions-1)){
            spills(i).closeInput()
          }
        }
        // result ++ groupingattributes ?
        val inputSchema = Seq(aggregatorSchema) ++ namedGroups.map(_._2)
        AggregateIteratorGenerator(resultExpression, inputSchema)(currentAggregationTable.iterator)

      }

      /**
        * Spill input rows to the proper partition using hashing
        *
        * @return
        */
      private def spillRecord(group: Row, row: Row) = {
        /* IMPLEMENT THIS METHOD */
        val joinedRow = new JoinedRow4(row, group)
        //                val index = joinedRow.hashCode() % spills.size
        val index = group.hashCode() % numPartitions
        spills(index).insert(row)

      }

      private def fetchSpill(): Boolean = {
        // IMPLEMENT ME
        //        // get row iterator of next Non-Empty partition
        //        while (!data.hasNext && partitionIterator.hasNext) {
        //          val thispartition = partitionIterator.next()
        //          data = thispartition.getData()
        //        }
        //
        //        if (!data.hasNext) {
        //          false
        //        }
        //        else {
        //
        //          // clear Aggregation Table and aggregateResult
        //          currentAggregationTable = new SizeTrackingAppendOnlyMap[Row, AggregateFunction]
        //          aggregateResult = aggregate()

//        for (i <- 0 to (numPartitions-1)){
//          spills(i).closePartition()
//        }

        while (!data.hasNext && diskPartitionIterator.hasNext) {
          data = diskPartitionIterator.next().getData()
        }
        if (data.hasNext) {
          currentAggregationTable = new SizeTrackingAppendOnlyMap[Row, AggregateFunction]
          aggregateResult = aggregate()
          true
        }
        else false


      }
    }
  }
}