```mermaid
classDiagram
  direction LR

  class FilterBase
  <<HBase>> FilterBase
  class SparkFilterProtos
  <<protobuf>> SparkFilterProtos

  class SparkSQLPushDownFilter
  class PushdownMappedField
  <<interface>> PushdownMappedField
  class ByteArrayComparable
  class DynamicLogicExpression
  <<trait>> DynamicLogicExpression
  class DynamicLogicExpressionBuilder
  <<object>> DynamicLogicExpressionBuilder
  class BytesEncoder
  <<trait>> BytesEncoder
  class JavaBytesEncoder
  <<Enumeration>> JavaBytesEncoder
  class BoundRanges
  class BoundRange

  class HBaseScanBuilder
  <<spark4>> HBaseScanBuilder
  class HBaseScan
  <<spark4>> HBaseScan
  class HBaseBatch
  <<spark4>> HBaseBatch
  class HBasePartitionReaderFactory
  <<spark4>> HBasePartitionReaderFactory
  class HBasePartitionReader
  <<spark4>> HBasePartitionReader
  class NaiveEncoder
  <<spark4>> NaiveEncoder
  class Field
  <<spark4>> Field

  SparkSQLPushDownFilter --|> FilterBase
  SparkSQLPushDownFilter o-- DynamicLogicExpression : expression
  SparkSQLPushDownFilter --> ByteArrayComparable : column maps
  SparkSQLPushDownFilter ..> PushdownMappedField : ctor input
  SparkSQLPushDownFilter ..> DynamicLogicExpressionBuilder : parseFrom
  SparkSQLPushDownFilter ..> JavaBytesEncoder : create
  SparkSQLPushDownFilter ..> SparkFilterProtos : serialize
  DynamicLogicExpression --> BytesEncoder : encoder
  DynamicLogicExpression ..> ByteArrayComparable : row values
  DynamicLogicExpression ..> JavaBytesEncoder : filterOps
  DynamicLogicExpressionBuilder ..> DynamicLogicExpression : builds tree
  BytesEncoder ..> BoundRanges : ranges
  BoundRanges *-- BoundRange : less, greater
  JavaBytesEncoder ..> BytesEncoder : create via reflection

  NaiveEncoder ..|> BytesEncoder
  HBaseScanBuilder ..> JavaBytesEncoder : create
  HBaseScanBuilder ..> HBaseScan : new
  HBaseScan --> BytesEncoder : encoder
  HBaseScan ..> BoundRanges : encoder ranges
  HBaseScan ..> HBaseBatch
  HBaseBatch ..> HBasePartitionReaderFactory
  HBasePartitionReaderFactory ..> HBasePartitionReader : new
  HBasePartitionReader ..> JavaBytesEncoder : create
  HBasePartitionReader ..> DynamicLogicExpression : builds tree
  HBasePartitionReader o-- SparkSQLPushDownFilter : pushDownFilter
  Field ..> PushdownMappedField : anon adapter
```