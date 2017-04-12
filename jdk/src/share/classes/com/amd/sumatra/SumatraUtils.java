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

package com.amd.sumatra;

import java.lang.reflect.Field;

public class SumatraUtils {
   public static Object getFieldFromObject(Field f, Object fromObj) {
      try {
         f.setAccessible(true);
         java.lang.reflect.Type type = f.getType();

         if (type == double.class) {
            return f.getDouble(fromObj);
         } else if (type == float.class) {
            return f.getFloat(fromObj);
         } else if (type == long.class) {
            return f.getLong(fromObj);
         } else if (type == int.class) {
            return f.getInt(fromObj);
         } else if (type == byte.class) {
            return f.getByte(fromObj);
         } else if (type == boolean.class) {
            return f.getBoolean(fromObj);
         } else {
            //object
            return f.get(fromObj);
         }
      } catch (Exception e) {
         //fail("unable to get field " + f + " from " + fromObj);
         return null;
      }

   }

   static volatile boolean offload = Boolean.getBoolean("com.amd.sumatra.offload.immediate");
   static volatile boolean neverRevertToJava = Boolean.getBoolean("com.amd.sumatra.neverRevertToJava");

   public static boolean getOffload() {
       return offload;
   }
   public static void setOffload(boolean offloadVal) {
       offload = offloadVal;
   }

   public static boolean getNeverRevertToJava() {
       return neverRevertToJava;
   }
   public static void setNeverRevertToJava(boolean offload) {
       neverRevertToJava = offload;
   }
}
