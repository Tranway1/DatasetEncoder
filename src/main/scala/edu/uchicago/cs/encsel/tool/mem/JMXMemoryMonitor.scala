/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package edu.uchicago.cs.encsel.tool.mem

import java.io.File
import java.lang.management.{ManagementFactory, MemoryMXBean, MemoryUsage}
import javax.management.ObjectName
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}

import com.sun.tools.attach.VirtualMachine

class JMXMemoryMonitor(vm: VirtualMachine) {

  var connectorAddr = vm.getAgentProperties().getProperty("com.sun.management.jmxremote.localConnectorAddress");
  if (connectorAddr == null) {
    val agent = vm.getSystemProperties().getProperty("java.home") +
      File.separator + "lib" + File.separator + "management-agent.jar";
    vm.loadAgent(agent);
    connectorAddr = vm.getAgentProperties().getProperty("com.sun.management.jmxremote.localConnectorAddress");
  }
  val serviceURL = new JMXServiceURL(connectorAddr);
  val connector = JMXConnectorFactory.connect(serviceURL);
  val mbsc = connector.getMBeanServerConnection();
  val objName = new ObjectName(ManagementFactory.MEMORY_MXBEAN_NAME);
  val memoryBean = ManagementFactory.newPlatformMXBeanProxy(mbsc, objName.toString(), classOf[MemoryMXBean]);

  def getHeapMemoryUsage: Option[MemoryUsage] = {
    try {
      return Some(memoryBean.getHeapMemoryUsage)
    } catch {
      case e: Exception => {
        return None
      }
    }
  }
}
