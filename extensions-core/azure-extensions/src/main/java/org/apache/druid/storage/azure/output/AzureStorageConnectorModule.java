/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.storage.azure.output;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.azure.AzureStorageDruidModule;

import java.util.Collections;
import java.util.List;

public class AzureStorageConnectorModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule(AzureStorageConnectorModule.class.getSimpleName())
            .registerSubtypes(AzureStorageConnectorProvider.class)
    );
  }

  @Override
  public void configure(Binder binder)
  {
    MapBinder<String, StorageConnector> mapBinder =
        PolyBind.optionBinder(binder, Key.get(StorageConnector.class));
    mapBinder.addBinding(AzureStorageDruidModule.SCHEME).toProvider(AzureStorageConnectorProvider.class);
    JsonConfigProvider.bind(
        binder,
        "druid.storage",
        AzureStorageConnectorProvider.class
    );
  }
}
