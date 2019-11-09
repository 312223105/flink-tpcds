/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.MultiJoinProjectTransposeRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;

public class FlinkMultiJoinProjectTransposeRule extends JoinProjectTransposeRule {

	public static final MultiJoinProjectTransposeRule MULTI_BOTH_PROJECT =
		new MultiJoinProjectTransposeRule(
			operand(LogicalJoin.class,
				operand(LogicalProject.class,
					operand(MultiJoin.class, any())),
				operand(LogicalProject.class,
					operand(MultiJoin.class, any()))),
			RelFactories.LOGICAL_BUILDER,
			"MultiJoinProjectTransposeRule: with two LogicalProject children");

	public static final MultiJoinProjectTransposeRule MULTI_LEFT_PROJECT =
		new MultiJoinProjectTransposeRule(
			operand(LogicalJoin.class,
				some(
					operand(LogicalProject.class,
						operand(MultiJoin.class, any())))),
			RelFactories.LOGICAL_BUILDER,
			"MultiJoinProjectTransposeRule: with LogicalProject on left");

	public static final MultiJoinProjectTransposeRule MULTI_RIGHT_PROJECT =
		new MultiJoinProjectTransposeRule(
			operand(LogicalJoin.class,
				operand(RelNode.class, any()),
				operand(LogicalProject.class,
					operand(MultiJoin.class, any()))),
			RelFactories.LOGICAL_BUILDER,
			"MultiJoinProjectTransposeRule: with LogicalProject on right");

	//~ Constructors -----------------------------------------------------------

	@Deprecated // to be removed before 2.0
	public FlinkMultiJoinProjectTransposeRule(
		RelOptRuleOperand operand,
		String description) {
		this(operand, RelFactories.LOGICAL_BUILDER, description);
	}

	/** Creates a MultiJoinProjectTransposeRule. */
	public FlinkMultiJoinProjectTransposeRule(
		RelOptRuleOperand operand,
		RelBuilderFactory relBuilderFactory,
		String description) {
		super(operand, description, false, relBuilderFactory);
	}

	//~ Methods ----------------------------------------------------------------

	@Override
	protected boolean hasLeftChild(RelOptRuleCall call) {
		return call.rels.length != 4;
	}

	@Override
	protected boolean hasRightChild(RelOptRuleCall call) {
		return call.rels.length > 3;
	}

	@Override
	protected Project getRightChild(RelOptRuleCall call) {
		if (call.rels.length == 4) {
			return call.rel(2);
		} else {
			return call.rel(3);
		}
	}

	@Override
	protected RelNode getProjectChild(
		RelOptRuleCall call,
		Project project,
		boolean leftChild) {
		// locate the appropriate MultiJoin based on which rule was fired
		// and which projection we're dealing with
		MultiJoin multiJoin;
		if (leftChild) {
			multiJoin = call.rel(2);
		} else if (call.rels.length == 4) {
			multiJoin = call.rel(3);
		} else {
			multiJoin = call.rel(4);
		}

		// create a new MultiJoin that reflects the columns in the projection
		// above the MultiJoin
		return FlinkRelOptUtil.projectMultiJoin(multiJoin, project);
	}
}
