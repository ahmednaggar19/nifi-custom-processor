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
package smartera.processors.demo;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.InputStreamReader;
import java.util.*;

@Tags({"record"})
@CapabilityDescription("Counts the occurrences of different values of records attribute " +
        "and aggregates that count over all passing flow files.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CountRecordAttribute extends AbstractProcessor {

    private static final PropertyDescriptor COUNTABLE_ATTRIBUTE = new PropertyDescriptor
            .Builder().name("COUNTABLE_ATTRIBUTE")
            .displayName("Countable Attribute")
            .description("The attribute which occurrences should be counted")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("If counts were update successfully")
            .build();

    private static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("orginal")
            .description("orginal flow files are directed to this relation")
            .build();

    private static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If the given countable attribute does not exist in the json record or if the input flow file" +
                    "is not a JSON object")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    /** Map containing attribute all values count.*/
    private Map <String, Integer> counts;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(COUNTABLE_ATTRIBUTE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);

        counts = new HashMap<>();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        String countableAttribute = context.getProperty(COUNTABLE_ATTRIBUTE).getValue();
        // reading json file and extract attribute value and update counters,
        session.read(flowFile, in -> {
            String inputText = IOUtils.toString(in);
            JSONObject jsonObject = null;
            try {
                jsonObject = new JSONObject(inputText);
            } catch (JSONException e) {
                getLogger().error("Input is not of type json : " + in.toString());
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
            String attributeValue = jsonObject.getString(countableAttribute);
            if (attributeValue == null) {
                getLogger().error("Attribute with name : " + countableAttribute + " does not exist in input record.");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
            incrementCount(attributeValue);
        });
        session.transfer(flowFile, REL_ORIGINAL);
        // writing output of all counters in map.
        FlowFile countsFile = session.create();
        countsFile = session.write(countsFile, out -> {
                JSONObject jsonObject = new JSONObject();
                for (String value : counts.keySet()) {
                    jsonObject.put(value, counts.get(value));
                }
                out.write(jsonObject.toString().getBytes("UTF-8"));
        });
        session.transfer(countsFile, REL_SUCCESS);
    }

    /** Increments the count for a given value for the attribute.
     * @param attributeValue value of attribute
     * */
    private void incrementCount(String attributeValue) {
        if (counts.containsKey(attributeValue)) {
            Integer count = counts.get(attributeValue);
            counts.put(attributeValue, count + 1);
        } else {
            counts.put(attributeValue, 1);
        }
    }
}
