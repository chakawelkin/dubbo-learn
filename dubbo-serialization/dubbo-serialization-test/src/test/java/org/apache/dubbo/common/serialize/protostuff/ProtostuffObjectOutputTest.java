/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.serialize.protostuff;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.List;

import org.apache.dubbo.common.serialize.model.SerializablePerson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

public class ProtostuffObjectOutputTest {

    private ByteArrayOutputStream byteArrayOutputStream;
    private ProtostuffObjectOutput protostuffObjectOutput;
    private ProtostuffObjectInput protostuffObjectInput;
    private ByteArrayInputStream byteArrayInputStream;

    @BeforeEach
    public void setUp() throws Exception {
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.protostuffObjectOutput = new ProtostuffObjectOutput(byteArrayOutputStream);
    }

    @Test
    public void testWriteObjectNull() throws IOException, ClassNotFoundException {
        this.protostuffObjectOutput.writeObject(null);
        this.flushToInput();

        assertThat(protostuffObjectInput.readObject(), nullValue());
    }

    @Test
    public void testSerializeTimestamp() throws IOException, ClassNotFoundException {
        Timestamp originTime = new Timestamp(System.currentTimeMillis());
        this.protostuffObjectOutput.writeObject(originTime);
        this.flushToInput();

        Timestamp serializedTime = protostuffObjectInput.readObject(Timestamp.class);
        assertThat(serializedTime, is(originTime));
    }

    @Test
    public void testSerializeSqlDate() throws IOException, ClassNotFoundException {
        java.sql.Date originTime = new java.sql.Date(System.currentTimeMillis());
        this.protostuffObjectOutput.writeObject(originTime);
        this.flushToInput();

        java.sql.Date serializedTime = protostuffObjectInput.readObject(java.sql.Date.class);
        assertThat(serializedTime, is(originTime));
    }

    @Test
    public void testObjectList() throws IOException, ClassNotFoundException {
        List<SerializablePerson> args = new ArrayList<SerializablePerson>();
        args.add(new SerializablePerson());

        this.protostuffObjectOutput.writeObject(args);
        this.flushToInput();

        List<SerializablePerson> serializedTime = (List<SerializablePerson>) protostuffObjectInput.readObject();
        assertThat(serializedTime, is(args));
    }

    @Test
    public void testCustomizeDateList() throws IOException, ClassNotFoundException {
        java.sql.Date originTime = new java.sql.Date(System.currentTimeMillis());
        java.sql.Date yesterdayTime = new java.sql.Date(System.currentTimeMillis() + 30*60*1000);
        java.sql.Date beforeTime = new java.sql.Date(System.currentTimeMillis() + 30*60*1000*4);
        List<java.sql.Date> list = new ArrayList<>();

        list.add(originTime);
        list.add(yesterdayTime);
        list.add(beforeTime);

        this.protostuffObjectOutput.writeObject(list);
        this.flushToInput();

        List<java.sql.Date> serializedTimeList = (List<java.sql.Date>) protostuffObjectInput.readObject();
        assertThat(serializedTimeList, is(list));
    }

    @Test
    public void testCustomizeTimeList() throws IOException, ClassNotFoundException {

        List<LocalTime> list = new ArrayList<LocalTime>();

        LocalTime localTime = LocalTime.parse("12:00:00");
        LocalTime localSecondTime = LocalTime.parse("13:00:00");
        LocalTime localThirdTime = LocalTime.parse("14:00:00");
        list.add(localTime);
        list.add(localSecondTime);
        list.add(localThirdTime);

        LocalTimeList timeList = new LocalTimeList(list);
        this.protostuffObjectOutput.writeObject(timeList);
        this.flushToInput();

        LocalTimeList serializedTime = protostuffObjectInput.readObject(LocalTimeList.class);
        assertThat(serializedTime, is(timeList));
    }

    @Test
    public void testListObject() throws IOException, ClassNotFoundException {

        List<SerializablePerson> list = new ArrayList<SerializablePerson>();

        list.add(new SerializablePerson());
        list.add(new SerializablePerson());
        list.add(new SerializablePerson());

        SerializablePersonList personList = new SerializablePersonList(list);

        this.protostuffObjectOutput.writeObject(personList);
        this.flushToInput();

        SerializablePersonList serializedTime = protostuffObjectInput.readObject(SerializablePersonList.class);
        assertThat(serializedTime, is(personList));
    }


    @Test
    public void testSerializeSqlTime() throws IOException, ClassNotFoundException {
        java.sql.Time originTime = new java.sql.Time(System.currentTimeMillis());
        this.protostuffObjectOutput.writeObject(originTime);
        this.flushToInput();

        java.sql.Time serializedTime = protostuffObjectInput.readObject(java.sql.Time.class);
        assertThat(serializedTime, is(originTime));
    }

    @Test
    public void testSerializeDate() throws IOException, ClassNotFoundException {
        Date originTime = new Date();
        this.protostuffObjectOutput.writeObject(originTime);
        this.flushToInput();

        Date serializedTime = protostuffObjectInput.readObject(Date.class);
        assertThat(serializedTime, is(originTime));
    }

    @Test
    public void testPojoMappingPollutionWithArrayList() throws IOException, ClassNotFoundException {
        // 模拟服务端：强制将ArrayList注册为POJO schema
//        Schema<ArrayList> schema = RuntimeSchema.getSchema(ArrayList.class);// 触发POJO注册
//        System.out.println("schema class: " + schema.getClass().getName());

        // 构造一个ArrayList
        ArrayList<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");

        // 序列化
        this.protostuffObjectOutput.writeObject(list);
        this.flushToInput();

        // 客户端：反序列化
        Object obj = protostuffObjectInput.readObject();
        assert obj instanceof ArrayList;
        ArrayList<?> deserialized = (ArrayList<?>) obj;

        // 这里尝试遍历，可能会抛出ConcurrentModificationException
        boolean gotException = false;
        try {
            for (Object o : deserialized) {
                // do nothing
            }
        } catch (ConcurrentModificationException e) {
            gotException = true;
        }
        assertThat(gotException, is(true));
    }

    @Test
    public void testSerializePojoMappingPollutionWithArrayList() throws IOException, ClassNotFoundException {
        // 这个是注册了ArrayList后序列化得到的二进制数组
        byte[] withRegisterBytes = new byte[] {
                0, 0, 0, 52, 0, 0, 0, 26, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 100, 117, 98, 98, 111, 46, 99, 111,
                109, 109, 111, 110, 46, 115, 101, 114, 105, 97, 108, 105, 122, 101, 46, 112, 114, 111, 116, 111, 115, 116, 117,
                102, 102, 46, 87, 114, 97, 112, 112, 101, 114, 11, -6, 7, 19, 106, 97, 118, 97, 46, 117, 116, 105, 108, 46, 65,
                114, 114, 97, 121, 76, 105, 115, 116, 8, 3, 12
        };

        // 这个是未注册ArrayList后序列化得到的二进制数组
        byte[] correctBytes = new byte[] {
                0, 0, 0, 52, 0, 0, 0, 29, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 100, 117, 98, 98, 111, 46, 99, 111,
                109, 109, 111, 110, 46, 115, 101, 114, 105, 97, 108, 105, 122, 101, 46, 112, 114, 111, 116, 111, 115, 116, 117,
                102, 102, 46, 87, 114, 97, 112, 112, 101, 114, 11, -54, 1, 9, 65, 114, 114, 97, 121, 76, 105, 115, 116, 11, 74,
                1, 97, 12, 11, 74, 1, 98, 12, 11, 74, 1, 99, 12, 12
        };

        // 是否注册到pojoMapping
        RuntimeSchema.getSchema(ArrayList.class);
        // 现在用一个没有注册的ArrayList的来反序列化，然后再遍历
        Object obj = new ProtostuffObjectInput(new ByteArrayInputStream(correctBytes)).readObject();

        assert obj instanceof ArrayList;
        ArrayList<?> deserialized = (ArrayList<?>) obj;

        boolean gotException = false;
        try {
            for (Object o : deserialized) {
                // do nothing
            }
        } catch (ConcurrentModificationException e) {
            gotException = true;
        }

        assertThat(gotException, is(true));
    }

    private void flushToInput() throws IOException {
        this.protostuffObjectOutput.flushBuffer();
        this.byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        this.protostuffObjectInput = new ProtostuffObjectInput(byteArrayInputStream);
    }

    private static class SerializablePersonList implements Serializable {
        private static final long serialVersionUID = 1L;

        public List<SerializablePerson> personList;

        public SerializablePersonList() {}

        public SerializablePersonList(List<SerializablePerson> list) {
            this.personList = list;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;

            SerializablePersonList list = (SerializablePersonList) obj;
            if (list.personList == null && this.personList == null)
                return true;
            if (list.personList == null || this.personList == null)
                return false;
            if (list.personList.size() != this.personList.size())
                return false;
            for (int i =0; i < this.personList.size(); i++) {
                if (!this.personList.get(i).equals(list.personList.get(i)))
                    return false;
            }
            return true;
        }
    }

    private static class LocalTimeList implements Serializable {
        private static final long serialVersionUID = 1L;

        List<LocalTime> timeList;

        public LocalTimeList() {}

        public LocalTimeList(List<LocalTime> timeList) {
            this.timeList = timeList;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;

            LocalTimeList timeList = (LocalTimeList) obj;
            if (timeList.timeList == null && this.timeList == null)
                return true;
            if (timeList.timeList == null || this.timeList == null)
                return false;
            if (timeList.timeList.size() != this.timeList.size())
                return false;
            for (int i =0; i < this.timeList.size(); i++) {
                if (!this.timeList.get(i).equals(timeList.timeList.get(i)))
                    return false;
            }
            return true;
        }
    }
}
