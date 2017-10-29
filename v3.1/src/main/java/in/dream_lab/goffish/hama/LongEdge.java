/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package in.dream_lab.goffish.hama;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.IEdge;

public class LongEdge<E extends Writable, I extends Writable, J extends Writable>
    implements IEdge<E, I, J> {

  private E _value;
  private long edgeID;
  private long _sink;

  LongEdge(long id, long sinkID) {
    edgeID = id;
    _sink = sinkID;
  }

  void setSinkID(long sinkID) {
    _sink = sinkID;
  }

  @Override
  public E getValue() {
    return _value;
  }

  @Override
  public void setValue(E val) {
    _value = val;
  }

  @Override
  public J getEdgeId() {
    return (J)new LongWritable(edgeID);
  }

  @Override
  public I getSinkVertexId() {
    return (I)new LongWritable(_sink);
  }
}
