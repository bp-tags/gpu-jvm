/*
 * Copyright (c) 2009, 2011, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package java.util.stream;

import java.util.Spliterators;
import java.util.Spliterator;
import java.util.concurrent.CountedCompleter;
import java.util.function.Supplier;

import com.amd.sumatra.SumatraUtils;
import java.util.function.IntConsumer;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.Class;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import com.oracle.graal.compiler.hsail.CompileAndDispatch;
import com.amd.sumatra.SumatraFactory;

import java.util.concurrent.ConcurrentHashMap;

public class PipelineInfo {

    // implemented by an ArrayList of PipelineInfo.Entry elements
    private ArrayList<Entry> plist = new ArrayList<>();

    public void add(Entry entry) {
        plist.add(entry);
    }

    public Entry get(int n) {
        return plist.get(n);
    }

    public int size() {
        return plist.size();
    }

    public void show() {
        int opNum = 0;
        for (Entry entry : plist) {
            opNum++;
            System.out.println("Op #" + opNum + entry);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PipelineInfo)) {
            return false;
        }
        PipelineInfo otherInfo = (PipelineInfo) other;
        return (plist.equals(otherInfo.plist));
    }

    @Override
    public int hashCode() {
        return plist.hashCode();
    }

    static public enum OpType {
        UNKNOWN,
        FILTER,
        MAP,
        PEEK,
        FOREACH,
        REDUCE
    };

    static public enum DataType {
        UNKNOWN,
        INT,
        OBJ
    };

    public static class Entry {
        public OpType op;               // what kind of op
        public DataType dtype;          // what type of data
        public Class<?> lambdaClass;    // class of lambda to call
        public Object lambdaObj;      // actual lambda object from this capture

        Entry(String klassName) {
            // hack alert, there should be a way to do this that is not dependent on the
            // order of the innerclass declarations
            if (klassName == "java.util.stream.ReduceOps$5ReducingSink") {
                setOpData(OpType.REDUCE, DataType.INT);
            } else if (klassName.endsWith("IntPipeline$9$1")) {
                setOpData(OpType.FILTER, DataType.INT);
            } else if (klassName.endsWith("IntPipeline$10$1")) {
                setOpData(OpType.PEEK, DataType.INT);
            } else if (klassName.endsWith("ReferencePipeline$2$1")) {
                setOpData(OpType.FILTER, DataType.OBJ);
            } else if (klassName.endsWith("ReferencePipeline$3$1")) {
                setOpData(OpType.MAP, DataType.OBJ);
            } else if (klassName.endsWith("ReferencePipeline$11$1")) {
                setOpData(OpType.PEEK, DataType.OBJ);
            } else if (klassName.contains("ForEachOps$ForEachOp$")) {
                setOpData(OpType.FOREACH, DataType.UNKNOWN);
            } else {
                System.out.println("WARNING: unmatched class name " + klassName + " in PipelineInfo.Entry constructor");
                setOpData(OpType.UNKNOWN, DataType.UNKNOWN);
            }
        }

        private void setOpData(OpType _op, DataType _dtype) {
            op = _op;
            dtype = _dtype;
        }

        @Override
        public boolean equals(Object other) {
            // on purpose we say that there are equal ignoring the lambdaObj fields
            if (!(other instanceof Entry)) {
                return false;
            }
            Entry otherEntry = (Entry) other;
            return ((op.equals(otherEntry.op))
                    && (dtype.equals(otherEntry.dtype))
                    && (lambdaClass.equals(otherEntry.lambdaClass)));
        }

        @Override
        public int hashCode() {
            // on purpose we don't hash in the lambdaObj field
            int hashOp = op.hashCode();
            int hashDtype = dtype.hashCode();
            int hashClass = lambdaClass.hashCode();
            return (hashOp + hashDtype + hashClass) * hashClass + hashOp;
        }

        public String toString() {
            return (", op=" + op + ", dtype=" + dtype + ", lambdaObj=" + lambdaObj);
        }
    }

    static Field findFieldStartsWith(Class<?> klass, String startStr) {
        Field[] fields = klass.getDeclaredFields();
        for (Field f : fields) {
            if (f.getName().startsWith(startStr)) {
                return f;
            }
        }
        // if we got this far,  no such field
        return null;
    }
    static boolean pipelineDebugPrint = Boolean.getBoolean("pipelineDebugPrint");

    static void debugPrint(String str) {
        if (pipelineDebugPrint) {
            System.out.println(str);
        }
    }

    public static <P_IN> PipelineInfo deducePipeline(Sink<P_IN> sink) {
        PipelineInfo info = new PipelineInfo();
        try {
            Object curSink = sink;
            int opNum = 1;
            while (curSink != null) {
                Class<?> curSinkKlass = curSink.getClass();
                String sinkKlassName = curSinkKlass.getName();
                Entry entry = new Entry(sinkKlassName);

                // val field is in this$1 for peek, filter etc
                Field thisField = findFieldStartsWith(curSinkKlass, "this$1");
                Field curSinkLambdaField = null;
                Object curSinkLambda = null;
                if (thisField != null) {

                    // Get pipeline object from sink
                    Object pipeline = SumatraUtils.getFieldFromObject(thisField, curSink);

                    // lambda is in val$cap$0 or arg$1
                    curSinkLambdaField = findFieldStartsWith(pipeline.getClass(), "val$");
                    curSinkLambda = SumatraUtils.getFieldFromObject(curSinkLambdaField, pipeline);
                } else if (sinkKlassName.startsWith("java.util.stream.ForEachOps$ForEachOp")) {
                    // It is a java.util.stream.ForEachOps$ForEachOp
                    curSinkLambdaField = findFieldStartsWith(curSinkKlass, "consumer");
                    if (curSinkLambdaField != null) {
                        curSinkLambda = SumatraUtils.getFieldFromObject(curSinkLambdaField, curSink);
                    }
                } else if (sinkKlassName.startsWith("java.util.stream.ReduceOps")) {
                    // *** Verify this is correct for more than IntStream reduce ***
                    // It is a java.util.stream.ReduceOps$5ReducingSink
                    curSinkLambdaField = findFieldStartsWith(curSinkKlass, "val$operator");
                    if (curSinkLambdaField != null) {
                        curSinkLambda = SumatraUtils.getFieldFromObject(curSinkLambdaField, curSink);
                    }
                }

                if (curSinkLambda == null) {
                    System.out.println("WARNING: cannot determine lambda from " + curSink);
                } else {
                    entry.lambdaObj = curSinkLambda;
                    entry.lambdaClass = curSinkLambda.getClass();
//                    System.out.println("entry.lambdaObj: " + entry.lambdaObj
//                            + " ... entry.lambdaClass=" + entry.lambdaClass);
                }

                // to move to the next downstream entry
                Class<?> curSinkSupKlass = curSinkKlass.getSuperclass();
                Field downstreamField = findFieldStartsWith(curSinkSupKlass, "downstream");
                Object downstream;
                if (downstreamField != null) {
                    Class<?> downstreamKlass = downstreamField.getType();
                    downstream = downstreamField.get(curSink);
                } else {
                    downstream = null;   // at the terminal op
                }
                info.add(entry);

                // for next iteration
                curSink = downstream;
                opNum++;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return info;
    }

    public static boolean isRangeIntSpliterator(Spliterator sp) {
        return (sp instanceof Streams.RangeIntSpliterator);
    }


    static final ConcurrentHashMap<PipelineInfo, Object> pipelineKernels = new ConcurrentHashMap<>();
    static final ConcurrentHashMap<PipelineInfo, Boolean> haveGoodPipelineKernel = new ConcurrentHashMap<>();

    static CompileAndDispatch sumatra = SumatraFactory.getSumatra();

    public static <P_OUT> void revertParallelForEachJava(Spliterator sp,
                                                         PipelineHelper<P_OUT> helper,
                                                         Object forEachOpInstance) {
        ((ForEachOps.ForEachOp) forEachOpInstance).evaluateParallelForEachJava(helper, sp);
    }

    static final String[] arrayBasedCollectionSpliteratorNames = {
        "java.util.ArrayList$ArrayListSpliterator",
        "java.util.Vector$VectorSpliterator",
        //"java.util.Spliterators$IntArraySpliterator"
    };

    static ArrayList<String> arrayBasedCollectionSpliterators;

    static {
        arrayBasedCollectionSpliterators = new ArrayList<String>(arrayBasedCollectionSpliteratorNames.length);
        for (int i = 0; i < arrayBasedCollectionSpliteratorNames.length; i++) {
            arrayBasedCollectionSpliterators.add(arrayBasedCollectionSpliteratorNames[i]);
        }
    }

    static <S> Spliterator offloadableSpliterator(Spliterator sp) {
        // don't have to check parallel, we got here from parallelForEach
        // any array spliterator is legal, I guess
        String spName = sp.getClass().getName();
        if (arrayBasedCollectionSpliterators.contains(spName)) {
            return sp;
        } else if (spName.equals("java.util.Spliterators$IntArraySpliterator")) {
            return sp;
        } else if (spName.equals("java.util.Spliterators$ArraySpliterator")) {
            return sp;
        } else if (PipelineInfo.isRangeIntSpliterator(sp)) {
            // certain kinds of intRangeSpliterators are ok
            Streams.RangeIntSpliterator risp = (Streams.RangeIntSpliterator) sp;
            if (risp.getFrom() == 0) {
                return sp;
            }
        }
        // if we got this far, it's not OK
        return null;
    }

    static final String NEWLINE = System.getProperty("line.separator");
    static final String REVERT_MSG = "WARNING: reverting to java, offload kernel could not be created."
                                     + NEWLINE + "Check HSAIL tools are on your PATH and LD_LIBRARY_PATH"
                                     + NEWLINE + "and on the sun.boot.library.path";


    static public String getIntReduceTargetName(Object intBinaryOp) {
        return sumatra.getIntReduceTargetName(intBinaryOp.getClass());
    }

    static public Integer offloadReducePipeline(Spliterator sp,
                                                     PipelineHelper<Integer> helper,
                                                     TerminalSink<Integer, Integer> sink,
                                                     Object reduceOpInstance,
                                                     String reducerName,
                                                     int identity) {
        PipelineInfo pipelineInfo = null;
        Boolean haveKernel;
        try {
            pipelineInfo = PipelineInfo.deducePipeline(sink);
            Object okraKernel = null;
            boolean success = false;
            // for now we only handle pipeline sizes of 1
            if (pipelineInfo.size() == 1) {
                haveKernel = haveGoodPipelineKernel.get(pipelineInfo);
                if (haveKernel == null) {
                    String code = sumatra.getIntegerReduceIntrinsic(reducerName);
                    okraKernel = sumatra.createKernelFromHsailString(code, reducerName);
                    if (okraKernel != null) {
                        // Store result for later call like kernels
                        pipelineKernels.put(pipelineInfo, okraKernel);
                        haveGoodPipelineKernel.put(pipelineInfo, true);
                    } else {
                        System.out.println(REVERT_MSG);
                        haveGoodPipelineKernel.put(pipelineInfo, false);
                    }
                } else if (haveKernel == true) {
                    okraKernel = pipelineKernels.get(pipelineInfo);
                }

                // now if we have a kernel, dispatch to it
                if (okraKernel != null) {
                    // source array
                    Field f;
                    f = sp.getClass().getDeclaredField("array");
                    int[] streamSource = (int[]) SumatraUtils.getFieldFromObject(f, sp);
                    int result = sumatra.offloadIntReduceImpl(okraKernel, identity, streamSource);
                    return result;
                }
            } else {
                System.out.println("WARNING: Pipeline size " + pipelineInfo.size() + " is too big to offload");
            }
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();

            if (pipelineInfo != null) {
                haveGoodPipelineKernel.put(pipelineInfo, false);
            }

        } catch (UnsatisfiedLinkError e) {
            System.err.println(e);
            e.printStackTrace();

            if (pipelineInfo != null) {
                haveGoodPipelineKernel.put(pipelineInfo, false);
            }
        }

        // If we get here revert to java
        return null;
    }


    static public <P_IN, P_OUT> void offloadForEachPipeline(Spliterator sp,
                                                     PipelineHelper<P_OUT> helper,
                                                     Sink<P_IN> sink,
                                                     Object forEachOpInstance) {

        PipelineInfo pipelineInfo = null;
        Boolean haveKernel;
        boolean isObjectLambda = false;
        boolean success = false;
        pipelineInfo = PipelineInfo.deducePipeline(sink);
        Object okraKernel = null;
        isObjectLambda = !PipelineInfo.isRangeIntSpliterator(sp);
        // for now we only handle pipeline sizes of 1
        if (pipelineInfo.size() == 1) {
            Object consumer = pipelineInfo.get(0).lambdaObj;
            Class consumerClass = consumer.getClass();
            try {
                haveKernel = haveGoodPipelineKernel.get(pipelineInfo);
                if (haveKernel == null) {
                    okraKernel = sumatra.createKernel(consumerClass);
                    if (okraKernel != null) {
                        // Store result for later call like kernels
                        pipelineKernels.put(pipelineInfo, okraKernel);
                        haveGoodPipelineKernel.put(pipelineInfo, true);
                    } else {
                        System.out.println(REVERT_MSG);
                        haveGoodPipelineKernel.put(pipelineInfo, false);
                    }
                } else if (haveKernel == true) {
                    okraKernel = pipelineKernels.get(pipelineInfo);
                }
            } catch (Exception | UnsatisfiedLinkError e) {
                // Kernel code gen exceptions will cause revert to Java
                System.err.println(e);
                e.printStackTrace();
                if (pipelineInfo != null) {
                    haveGoodPipelineKernel.put(pipelineInfo, false);
                }
                forEachOpenCLPipelineRevertToJava(sp, helper, forEachOpInstance);
                return;
            }

            // now if we have a kernel, dispatch to it
            if (okraKernel != null) {
                // Extract actual args from Consumer
                Field[] fields = consumerClass.getDeclaredFields();
                ArrayList<Object> args = new ArrayList<Object>();
                int argIndex = 0;
                for (Field f : fields) {
                    //logger.info("... " + f);
                    args.add(SumatraUtils.getFieldFromObject(f, consumer));
                }

                // Secretly pass in the source array reference, each element
                // will be retrieved using the workitem id
                if (isObjectLambda) {
                    Field f;
                    String spName = sp.getClass().getName();
                    try {
                        if (arrayBasedCollectionSpliterators.contains(spName) == true) {
                            Field listField = sp.getClass().getDeclaredField("list");
                            AbstractList list = (AbstractList) SumatraUtils.getFieldFromObject(listField, sp);
                            // We want ArrayList.elementData
                            f = list.getClass().getDeclaredField("elementData");
                            args.add(SumatraUtils.getFieldFromObject(f, list));
                        } else {
                            // We want ArraySpliterator.array
                            f = sp.getClass().getDeclaredField("array");
                            args.add(SumatraUtils.getFieldFromObject(f, sp));
                        }
                    } catch (NoSuchFieldException e) {
                        // Decoding the pipeline failed here, revert to Java
                        System.err.println(e);
                        e.printStackTrace();
                        if (pipelineInfo != null) {
                            haveGoodPipelineKernel.put(pipelineInfo, false);
                        }
                        forEachOpenCLPipelineRevertToJava(sp, helper, forEachOpInstance);
                        return;
                    }
                }
                sumatra.dispatchKernel(okraKernel, (int) sp.estimateSize(), args.toArray());
                /*
                 With HSAIL deoptimization, a deopting/throwing kernel would have aborted
                 and finished running the workitems on the CPU. There may be a pending exception
                 that will be propagated back up to the caller.
                 */
            }
        } else {
            // Pipeline size > 1
            forEachOpenCLPipelineRevertToJava(sp, helper, forEachOpInstance);
        }
    }


    static <P_OUT> void forEachOpenCLPipelineRevertToJava(Spliterator sp,
                                                          PipelineHelper<P_OUT> helper,
                                                          Object forEachOpInstance) {

        // the following flag would normally be set in testing mode
        // when we want to consider the need to revert to java a failure
        if (SumatraUtils.getNeverRevertToJava()) {
            // cause an unchecked exception
            throw new RuntimeException("configured to never revert to Java");
        }

        // get here if it is OK to revert to Java
        // call back to the java fork-join version
        // (call back thru PipelineInfo to avoid having to make the target class public)
        PipelineInfo.revertParallelForEachJava(sp, helper, forEachOpInstance);
    }

}
