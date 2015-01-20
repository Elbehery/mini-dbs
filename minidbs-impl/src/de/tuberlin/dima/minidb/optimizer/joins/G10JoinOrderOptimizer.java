package de.tuberlin.dima.minidb.optimizer.joins;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.optimizer.joins.util.JoinOrderOptimizerUtils;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.JoinGraphEdge;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateConjunct;

public class G10JoinOrderOptimizer implements JoinOrderOptimizer {

	private CardinalityEstimator estimator;
	
	
	

	public G10JoinOrderOptimizer(CardinalityEstimator estimator) {
		this.estimator = estimator;
		
		
	}

	@Override
	public OptimizerPlanOperator findBestJoinOrder(Relation[] relations,
			JoinGraphEdge[] joins) {
		
		ArrayList<G10Plan[]> plansArray;
		
		
		plansArray = new ArrayList<G10Plan[]>();
		
		
		G10Plan[] singleRelationPlans = new G10Plan[relations.length];
		
		
		// Set ids for relations to simplify futur work
		for (int i = 0; i < relations.length; i++) {
			Relation relation = relations[i];
			relation.setID(i);
			
			estimator.estimateTableAccessCardinality((BaseTableAccess)relation);
			
			
		}
		
		JoinPredicateConjunct predicates = new JoinPredicateConjunct();
		//System.out.println("Graph preds : ");
		for (JoinGraphEdge join : joins) {
			//System.out.println(join.getJoinPredicate());
			predicates.addJoinPredicate(join.getJoinPredicate());
		}
		
		
		JoinPredicateConjunct filteredPredicates = (JoinPredicateConjunct) JoinOrderOptimizerUtils.filterTwinPredicates(predicates);
		
	//	System.out.println("Preds filtered : " + filteredPredicates.toString());
		
		
		
		
		
		for (int i = 0; i < relations.length; i++) {
			Relation relation = relations[i];
			
			int id = relation.getID();
			int relationBitmap = (int) Math.pow(2, id);
			int neighbourBitmap = 0;
			
			for (JoinGraphEdge join : joins) {
				if(join.getLeftNode().getID() == id)
					neighbourBitmap += Math.pow(2, join.getRightNode().getID());
				else if(join.getRightNode().getID() == id)
					neighbourBitmap += Math.pow(2, join.getLeftNode().getID());					
			}
			G10Plan plan = new G10Plan(relation, relationBitmap, neighbourBitmap);
			
			singleRelationPlans[i] = plan;
		}
		
		plansArray.add(singleRelationPlans);
		
		
		
		
		for(int size = 2; size <= relations.length; size ++) {
			
			printPlans(plansArray);
			// Group the new plans according to their relation bitmap to be able to keep only the best one more easily
			HashMap<Integer, ArrayList<G10Plan>> resultPlans = new HashMap<Integer, ArrayList<G10Plan>>();
			
			for (int i = 1; i <= size - i; i++) {
				
				// Build plans as join of plans of size i and N-i
				G10Plan[] plansRight = plansArray.get(i-1);
				G10Plan[] plansLeft = plansArray.get(size - i -1);
				
				
				
				for(G10Plan planLeft : plansLeft) {
					for(G10Plan planRight : plansRight) {
						
						// Check if the 2 plans are neighbours
						if((planLeft.getNeighbourBitmap() & planRight.getRelationBitmap()) != 0) {
							
							// Check that the 2 plans have no relations in common
							if((planLeft.getRelationBitmap() & planRight.getRelationBitmap()) == 0) {
								
								
								// Merge the 2 plans
								int relationBitmap = planLeft.getRelationBitmap() | planRight.getRelationBitmap();
								int neighbourBitmap = (planLeft.getNeighbourBitmap() | planRight.getNeighbourBitmap()) &(~relationBitmap);
								
								
								// Compute the resulting predicate
								JoinPredicateConjunct joinPred = new JoinPredicateConjunct();
								for (JoinGraphEdge join : joins) {
									int idL = join.getLeftNode().getID();
									int idR = join.getRightNode().getID();
							
									
									if ((planLeft.getRelationBitmap() >> idL) %2 != 0 && (planRight.getRelationBitmap() >> idR) %2 != 0 ) {
										
								//		if (filteredPredicates.contains(join.getJoinPredicate()))
											joinPred.addJoinPredicate(join.getJoinPredicate());
										
									} else if ((planRight.getRelationBitmap() >> idL) %2 != 0 && (planLeft.getRelationBitmap() >> idR) %2 != 0 ) {
										
								//		if (filteredPredicates.contains(join.getJoinPredicate()))
											joinPred.addJoinPredicate(join.getJoinPredicate());
									}
								}
								/*
								
								OptimizerPlanOperator pLeft = planLeft.getPlan();
								
								if (pLeft instanceof AbstractJoinPlanOperator) {
									AbstractJoinPlanOperator joinLeft = (AbstractJoinPlanOperator) pLeft;
									
									joinPred.addJoinPredicate(joinLeft.getJoinPredicate());
								}
								
								OptimizerPlanOperator pRight = planLeft.getPlan();
								
								if (pLeft instanceof AbstractJoinPlanOperator) {
									AbstractJoinPlanOperator joinRight = (AbstractJoinPlanOperator) pRight;
									
									joinPred.addJoinPredicate(joinRight.getJoinPredicate());
								}
								
								*/
								// Test isn't compliant if predicates are switched ...
								//JoinPredicate joinPredFiltered = joinPred.createSideSwitchedCopy();
								
								
								// Filter the predicates according to the already used predicates
								
								
								JoinPredicateConjunct globalPred = getGlobalPredicates(planLeft.getPlan(), planRight.getPlan());
								
								JoinPredicateConjunct probePred = new JoinPredicateConjunct();

								probePred.addJoinPredicate(globalPred);
								probePred.addJoinPredicate(joinPred);
								
								int probeSize = probePred.getConjunctiveFactors().size();
								
								JoinPredicateConjunct finalJoinPred = new JoinPredicateConjunct();
								
								JoinPredicate filteredPred = JoinOrderOptimizerUtils.filterTwinPredicates(probePred);

								
								if (filteredPred instanceof JoinPredicateAtom) {
									if (probeSize == 1)
										finalJoinPred.addJoinPredicate(joinPred);
								} else {
									
									if (((JoinPredicateConjunct)filteredPred).getConjunctiveFactors().size() == probeSize) {
										
										finalJoinPred.addJoinPredicate(joinPred);
									} else {
										
										for (int j = 0; j < joinPred.getConjunctiveFactors().size() - 1; j++) {
											finalJoinPred.addJoinPredicate(joinPred.getConjunctiveFactors().get(j));
										}
									}
								}
								
							/*	int globalPredSize = globalPred.getConjunctiveFactors().size();
								
								for (JoinPredicateAtom pred : joinPred.getConjunctiveFactors()) {
									JoinPredicateConjunct probePred = new JoinPredicateConjunct();
									probePred.addJoinPredicate(globalPred);
									probePred.addJoinPredicate(pred);
									
									JoinPredicate filteredPred = JoinOrderOptimizerUtils.filterTwinPredicates(probePred);
									
									
									
									if( filteredPred instanceof JoinPredicateAtom && globalPredSize == 0) {
										
										finalJoinPred.addJoinPredicate(pred);
										
									//	globalPred.addJoinPredicate(pred);
									//	globalPredSize++;
									}
									
									else if (filteredPred instanceof JoinPredicateConjunct) {
										int filteredSize = ((JoinPredicateConjunct) filteredPred).getConjunctiveFactors().size();
										System.out.println("Filtering : " + (globalPredSize + 1) + " to " + filteredSize );
										if (filteredSize == globalPredSize +1) {
											finalJoinPred.addJoinPredicate(pred);
											//globalPred.addJoinPredicate(pred);
											//globalPredSize++;
									}
									}
									
								}*/
								
								
								
							/*	JoinPredicateConjunct probePred = new JoinPredicateConjunct();
								
								
								
								probePred.addJoinPredicate(globalPred);
								probePred.addJoinPredicate(joinPred);
								
								System.out.println("Probe : " + probePred.toString());
								
								
								
								System.out.println("Filtered : " + filteredPred.toString());
								
								if (filteredPred instanceof JoinPredicateConjunct) {
									JoinPredicateConjunct filteredPredConj = (JoinPredicateConjunct) filteredPred;
									
									for (JoinPredicateAtom pred : joinPred.getConjunctiveFactors()) {
										if (filteredPredConj.contains(pred))
											finalJoinPred.addJoinPredicate(pred);									
									}
								} else {
									JoinPredicateAtom filteredPredConj = (JoinPredicateAtom) filteredPred;
									
									for (JoinPredicateAtom pred : joinPred.getConjunctiveFactors()) {
										if (filteredPredConj.equals(pred))
											finalJoinPred.addJoinPredicate(pred);									
									}
								}
								
								
								*/
								
								
								
							/*	JoinPredicate joinPredFiltered;;
								if (joinPred.getConjunctiveFactors().size() > 2) {
									
									System.out.println("Many preds ! : ");
									for (JoinPredicate pred : joinPred.getConjunctiveFactors()) {
										System.out.println(pred.toString());
									}
									
									joinPredFiltered = JoinOrderOptimizerUtils.filterTwinPredicates(joinPred);

									System.out.println("Filtered! : ");
									for (JoinPredicate pred : ((JoinPredicateConjunct)joinPredFiltered).getConjunctiveFactors()) {
										System.out.println(pred.toString());
									}
									
									
									
								} else if (joinPred.getConjunctiveFactors().size() > 0) {
									joinPredFiltered = JoinOrderOptimizerUtils.filterTwinPredicates(joinPred);
								} else {
									joinPredFiltered = new JoinPredicateConjunct();
								}*/
									
								// Create the plan
								OptimizerPlanOperator planOLeft = planLeft.getPlan();
								OptimizerPlanOperator planORight = planRight.getPlan();
								
								AbstractJoinPlanOperator planOperator = new AbstractJoinPlanOperator(planOLeft, planORight, finalJoinPred);
							
								
								estimator.estimateJoinCardinality(planOperator);
								
								planOperator.setOperatorCosts(planORight.getOutputCardinality() + planOLeft.getOutputCardinality());
								
								planOperator.setCumulativeCosts(planOperator.getOperatorCosts() + planOLeft.getCumulativeCosts() + planORight.getCumulativeCosts());
							
								G10Plan plan = new G10Plan(planOperator, relationBitmap, neighbourBitmap);
								
								if (resultPlans.containsKey(relationBitmap))
									resultPlans.get(relationBitmap).add(plan);
								else {
									ArrayList<G10Plan> planList = new ArrayList<G10Plan>();
									planList.add(plan);
									resultPlans.put(relationBitmap, planList);
									
								}
								System.out.println("Added plan " + relationBitmap + " (cost " + planOperator.getCumulativeCosts() + ")");

							}
						}
						
						
					}				
				}
				
			}
			
			// Remove bad plans
			
			G10Plan[] goodPlans = filterPlans(resultPlans);
			
			plansArray.add(goodPlans);
				
		}
	
		
		// Return best plan
		
		
		AbstractJoinPlanOperator bestPlan = (AbstractJoinPlanOperator) plansArray.get(plansArray.size() -1)[0].getPlan();
		
		System.out.println("Best plan : ");
		
		JoinPredicateConjunct preds = getGlobalPredicates(bestPlan.getLeftChild(), bestPlan.getRightChild());
		
		preds.addJoinPredicate(bestPlan.getJoinPredicate());
		System.out.println("Preds : " + preds.toString());
		
		printPlan(bestPlan);
		
		return plansArray.get(plansArray.size() -1)[0].getPlan();
		

	}
	
	
	
	private JoinPredicateConjunct getGlobalPredicates(OptimizerPlanOperator planLeft, OptimizerPlanOperator planRight) {
		
		JoinPredicateConjunct globalPred = new JoinPredicateConjunct();
		
		
		if (planLeft instanceof AbstractJoinPlanOperator)
			globalPred.addJoinPredicate(((AbstractJoinPlanOperator)planLeft).getJoinPredicate());	
		
		if (planRight instanceof AbstractJoinPlanOperator)
			globalPred.addJoinPredicate(((AbstractJoinPlanOperator)planRight).getJoinPredicate());	

		
		Iterator<OptimizerPlanOperator> itLeft = planLeft.getChildren();
		Iterator<OptimizerPlanOperator> itRight = planRight.getChildren();
		
		while (itLeft.hasNext()) {
			OptimizerPlanOperator childLeft = itLeft.next();
			if(childLeft instanceof AbstractJoinPlanOperator)
				globalPred.addJoinPredicate(((AbstractJoinPlanOperator)childLeft).getJoinPredicate());			
		}
		
		while (itRight.hasNext()) {
			OptimizerPlanOperator childRight = itRight.next();
			if(childRight instanceof AbstractJoinPlanOperator)
				globalPred.addJoinPredicate(((AbstractJoinPlanOperator)childRight).getJoinPredicate());			
		}
		
		
		return globalPred;
		
		
	}
	
	
	private void printPlan(OptimizerPlanOperator plan) {
		
		if (plan instanceof Relation) {
			
		//	System.out.println("Relation : " + ((Relation) plan).toString());
			
		} else {
			
			
	//		System.out.println("Join : ");
			
			
			
			
			AbstractJoinPlanOperator joinPlan = (AbstractJoinPlanOperator) plan;
			OptimizerPlanOperator planLeft = joinPlan.getLeftChild();
			OptimizerPlanOperator planRight = joinPlan.getRightChild();
			
		//	System.out.println(joinPlan.getJoinPredicate().toString());
			
			this.estimator.estimateJoinCardinality(joinPlan);
			
			printPlan(planLeft);
			printPlan(planRight);
			
			
			
		}
		
		
	}
	
	private void printPlans(ArrayList<G10Plan[]> plansArray) {
		
		System.out.println();
		System.out.print(0 + " : ");
		
		G10Plan[] planArray = plansArray.get(0);
		for (int j = 0; j < planArray.length; j++) {
			G10Plan plan = planArray[j];
			System.out.print(plan.getRelationBitmap() + " | " + plan.getNeighbourBitmap() + "(" + plan.getPlan().getOutputCardinality() + "), ");
		}
		
		for (int i = 1; i < plansArray.size(); i++) {
			System.out.println();
			System.out.print(i + " : ");
			
			planArray = plansArray.get(i);
			

	
			
			for (int j = 0; j < planArray.length; j++) {
				G10Plan plan = planArray[j];
				System.out.print(plan.getRelationBitmap() + " | " + plan.getNeighbourBitmap() + "(" + plan.getPlan().getOperatorCosts() + " + "+ plan.getPlan().getCumulativeCosts() + "), ");
			}
		}
	}
	
	
	
	private G10Plan[] filterPlans(HashMap<Integer, ArrayList<G10Plan>> plans) {
		
		
		ArrayList<G10Plan> outputPlans = new ArrayList<G10Plan>();
		
		for (ArrayList<G10Plan> planList : plans.values()) {
			
			G10Plan minPlan = planList.get(0);
			long minCost = minPlan.getPlan().getCumulativeCosts();
			
			for (int i = 1; i < planList.size(); i++) {
				G10Plan plan = planList.get(i);
				if (plan.getPlan().getCumulativeCosts() < minCost) {
					minCost = plan.getPlan().getCumulativeCosts();
					minPlan = plan;
				}
			}
			outputPlans.add(minPlan);
			System.out.println("Best : " + minPlan.getRelationBitmap());
		}
		
		return  (G10Plan[]) outputPlans.toArray(new G10Plan[outputPlans.size()]);
		
		
	}
	
	
	
	private class G10Plan {
		
		private OptimizerPlanOperator plan;
		private int relationBitmap;
		private int neighbourBitmap;
		
		public G10Plan(OptimizerPlanOperator plan, int relationBitmap, int neighbourBitmap) {
			this.plan = plan;
			this.relationBitmap = relationBitmap;
			this.neighbourBitmap = neighbourBitmap;		
		}
		
		public int getNeighbourBitmap() {
			return this.neighbourBitmap;
		}
		
		public int getRelationBitmap() {
			return this.relationBitmap;
		}
		
		public OptimizerPlanOperator getPlan() {
			return plan;
		}
		
		
	}

}
