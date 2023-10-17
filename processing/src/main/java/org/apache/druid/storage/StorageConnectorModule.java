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

package org.apache.druid.storage;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.storage.local.LocalFileStorageConnector;
import org.apache.druid.storage.local.LocalFileStorageConnectorConfig;
import org.apache.druid.storage.local.LocalFileStorageConnectorProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

public class StorageConnectorModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(StorageConnector.class.getSimpleName())
            .registerSubtypes(LocalFileStorageConnectorProvider.class)
    );
  }

  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoiceWithFallback(
        binder,
        "druid.storage.type",
        Key.get(StorageConnector.class),
        Key.get(NoopStorageConnector.class)
    );
    MapBinder<String, StorageConnector> mapBinder =
        PolyBind.optionBinder(binder, Key.get(StorageConnector.class));
    mapBinder.addBinding("local").to(LocalFileStorageConnector.class);
    JsonConfigProvider.bind(
        binder,
        "druid.storage",
        LocalFileStorageConnectorConfig.class
    );
  }

  private static class NoopStorageConnector implements StorageConnector
  {
    private static final String ERROR_MSG = "Please provide a storage connector config.";

    @Override
    public boolean pathExists(String path) throws IOException
    {
      throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public InputStream read(String path) throws IOException
    {
      throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public InputStream readRange(String path, long from, long size) throws IOException
    {
      throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public OutputStream write(String path) throws IOException
    {
      throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public void deleteFile(String path) throws IOException
    {
      throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public void deleteFiles(Iterable<String> paths) throws IOException
    {
      throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public void deleteRecursively(String path) throws IOException
    {
      throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public Iterator<String> listDir(String dirName) throws IOException
    {
      throw new UnsupportedOperationException(ERROR_MSG);
    }
  }
}
