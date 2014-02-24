/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mondrian.calc.Calc;
import mondrian.mdx.LevelExpr;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.Formula;
import mondrian.olap.FunTable;
import mondrian.olap.MondrianDef;
import mondrian.olap.Parameter;
import mondrian.olap.Property;
import mondrian.olap.Query;
import mondrian.olap.QueryAxis;
import mondrian.olap.QueryPart;
import mondrian.olap.SchemaReader;
import mondrian.olap.ValidatorImpl;
import mondrian.olap.fun.DescendantsFunDef;
import mondrian.olap.fun.DescendantsFunDef.Flag;
import mondrian.olap.fun.FunUtil;
import mondrian.olap.fun.LevelMembersFunDef;
import mondrian.resource.MondrianResource;
import mondrian.server.Statement;

/**
 * While under development, this class holds the majority of the logic for calculated cells, to reduce merge impacts across the various classes in Mondrian.
 * 
 * @author Will Gorman <wgorman@pentaho.com>
 *
 */
public class CalculatedCellUtil {

    /**
     * Note, this function needs to be highly performant.  Ideas for increasing performance include:
     *  - transitioning the interpretation of expressions into calculations during schema time
     *  - look at prioritizing the cube expressions in an order based on cardinality / usage / etc
     *  - review role based security processing for performance ideas
     *  
     * @param calc
     * @return
     */
    public static boolean insideSubcube(RolapEvaluator eval, CellCalc calc) {
        for (int i = 0; i < calc.cubeExps.length; i++) {
            if (calc.cubeExps[i] instanceof MemberExpr) {
                RolapMember member = (RolapMember)((MemberExpr)calc.cubeExps[i]).getMember();
                final int ordinal = member.getHierarchy().getOrdinalInCube();
                final RolapMember curr = (RolapMember)eval.getMembers()[ordinal];
                if (!curr.equals(member)) {
                    return false;
                }
            } else if (calc.cubeExps[i] instanceof ResolvedFunCall && ((ResolvedFunCall)calc.cubeExps[i]).getFunDef() instanceof LevelMembersFunDef) {
                RolapCubeLevel lvl = (RolapCubeLevel)((LevelExpr)((ResolvedFunCall)calc.cubeExps[i]).getArg(0)).getLevel();
                final int ordinal = lvl.getHierarchy().getOrdinalInCube();
                final RolapMember curr = (RolapMember)eval.getMembers()[ordinal];
                if (!curr.getLevel().equals(lvl)) {
                    return false;
                }
            } else if (calc.cubeExps[i] instanceof ResolvedFunCall && ((ResolvedFunCall)calc.cubeExps[i]).getFunDef() instanceof DescendantsFunDef) {
                if (((ResolvedFunCall)calc.cubeExps[i]).getArgCount() == 1 && ((ResolvedFunCall)calc.cubeExps[i]).getArg( 0 ) instanceof MemberExpr) {
                    RolapMember member = (RolapMember)((MemberExpr)((ResolvedFunCall)calc.cubeExps[i]).getArg( 0 )).getMember();
                    final int ordinal = member.getHierarchy().getOrdinalInCube();
                    final RolapMember curr = (RolapMember)eval.getMembers()[ordinal];
                    if (!curr.isChildOrEqualTo(member)) {
                        return false;
                    }
                } else if (((ResolvedFunCall)calc.cubeExps[i]).getArg(0) instanceof MemberExpr) {
                    // first grab the member object
                    RolapMember member = (RolapMember)((MemberExpr)((ResolvedFunCall)calc.cubeExps[i]).getArg( 0 )).getMember();
                    final int ordinal = member.getHierarchy().getOrdinalInCube();
                    final RolapMember curr = (RolapMember)eval.getMembers()[ordinal];
                    
                    // all of the checks below need to make sure the member is at least a descendant.
                    if (!curr.isChildOrEqualTo( member)) {
                        return false;
                    }
                    RolapCubeLevel level = null;
                    if (((ResolvedFunCall)calc.cubeExps[i]).getArg(1) instanceof LevelExpr) {
                        level = (RolapCubeLevel)((LevelExpr)((ResolvedFunCall)calc.cubeExps[i]).getArg( 1 )).getLevel();
                    } else {
                        // TODO: Support Numeric Expressions (Level Depth)
                        throw new UnsupportedOperationException();
                    }
                    Flag flag = Flag.SELF;
                    if (((ResolvedFunCall)calc.cubeExps[i]).getArgCount() > 2) {
                        flag = FunUtil.getLiteralArg((ResolvedFunCall)calc.cubeExps[i], 2, Flag.SELF, Flag.class);
                    }
                    if (flag.leaves) {
                        if (curr.getLevel().getHierarchy().getLevels()[curr.getLevel().getHierarchy().getLevels().length - 1]
                                != curr.getLevel()) {
                            return false;
                        }
                    } else {
                        if (flag.self) {
                            if (curr.getLevel().getDepth() == level.getDepth()) {
                                continue;
                            }
                        }
                        if (flag.before) {
                            if (curr.getLevel().getDepth() < level.getDepth()) {
                                continue;
                            }
                        }
                        if (flag.after) {
                            if (curr.getLevel().getDepth() > level.getDepth()) {
                                continue;
                            }
                        }
                        return false;
                    }
                } else {
                    // TODO: Throw better exception when sets are in play - check during schema time vs. runtime.
                    throw new UnsupportedOperationException();
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }
        return true;
    }
    
    /**
     * This method is called by RolapEvaluator to determine which cell calculations apply to the current context. 
     * 
     * @param The evaluator
     * 
     * @return a list of cell calculations to apply.
     */
    public static List<CellCalc> applyCellCalculations(RolapEvaluator eval) {
        List<CellCalc> currentCellCalcs = null;
        if (eval.getCube().cellCalcs != null) {
            for (CellCalc calc : eval.getCube().cellCalcs) {
              if (!eval.root.activeCellCalcs.contains(calc) && insideSubcube(eval, calc)) {
                  // track that we are inside this subcube, otherwise
                  // this calculation would get executed repeatedly.
                  eval.root.activeCellCalcs.add(calc);
      
                  // lazy init to avoid extra memory use and object creation
                  if (currentCellCalcs == null) {
                    currentCellCalcs = new ArrayList<CellCalc>(); 
                  }
                  currentCellCalcs.add(calc);
                  final Calc calcVal = eval.root.getCompiled(calc.cellExp, true, null);
                  RolapCellCalculation calcObj = new RolapCellCalculation(calcVal, calc.solve_order);
                  // adds the cell calculation to the evaluator.
                  eval.addCalculation(calcObj, true);                
                } 
            }
        }
        return currentCellCalcs;
    }

    /**
     * This class encapsulates a cell calculation for the evaluator.
     */
    static class RolapCellCalculation implements RolapCalculation {
        int calcSolveOrder;
        Calc calcVal;
        
        public RolapCellCalculation(Calc calcVal, int calcSolveOrder) {
            this.calcVal = calcVal;
            this.calcSolveOrder = calcSolveOrder;
        }
        
        public void setContextIn( RolapEvaluator evaluator ) {
            evaluator.removeCalculation(this, true);
        }
    
        public int getSolveOrder() {
            return calcSolveOrder;
        }
    
        public int getHierarchyOrdinal() {
            // This calc is not associated with a hierarchy, it should be calculated first if
            // there is a solve order tie.
            return -1;
        }
    
        public boolean isCalculatedInQuery() {
            return false;
        }
    
        public Calc getCompiledExpression( RolapEvaluatorRoot root ) {
            return calcVal;
        }
    
        public boolean containsAggregateFunction() {
            throw new UnsupportedOperationException();
        }
    };
    
    /**
     * This function is called by cubes and virtual cubes during initialization
     *
     * @param The cube initializing
     * @param The calculated cells defined in the schema for this cube
     * @param a list to populate with processed cell calculations
     */
    public static void processCalculatedCells(RolapCube cube, MondrianDef.CalculatedCell[] calculatedCells, List<CellCalc> cellCalcs) {
        if (calculatedCells == null) return;
        for (int i = 0; i < calculatedCells.length; i++) {
            final String subcubeString = calculatedCells[i].subcube.cdata;
            final String cellString = calculatedCells[i].formula.cdata;
            final CellCalc cellCalc = new CellCalc();
            try {
                cellCalc.cubeExp = cube.getSchema().getInternalConnection().parseExpression(subcubeString);
                cellCalc.cellExp = cube.getSchema().getInternalConnection().parseExpression(cellString);
                for (MondrianDef.CalculatedCellProperty prop : calculatedCells[i].cellProperties) {
                    if (prop.name.equals(Property.SOLVE_ORDER.name)) {
                        try {
                            cellCalc.solve_order = Integer.parseInt(prop.value);
                        } catch (NumberFormatException e) {
                            // not a number, ignore
                        }
                    }
                }
                CellValidator valid = new CellValidator(cube.getSchema().getFunTable(), cube.getSchemaReader(), cube);
                if (((UnresolvedFunCall)cellCalc.cubeExp).getFunName().equals("()")) {
                    cellCalc.cubeExps = new Exp[((UnresolvedFunCall)cellCalc.cubeExp).getArgCount()];
                    for (int j = 0; j < ((UnresolvedFunCall)cellCalc.cubeExp).getArgCount(); j++) {
                      cellCalc.cubeExps[j] = valid.validate( ((UnresolvedFunCall)cellCalc.cubeExp).getArg(j), false );
                    }
                } else {
                    throw new UnsupportedOperationException();
                }
                cellCalc.cellExp = valid.validate( cellCalc.cellExp, false);
                cellCalcs.add( cellCalc );
            } catch (Exception e) {
                throw MondrianResource.instance().NamedSetHasBadFormula.ex(
                    subcubeString, e);
            }
        }
    }

    /**
     * This class is used to process the cell calculation subcube and formula definitions.
     */
    private static class CellValidator extends ValidatorImpl {
        SchemaReader reader;
        Query query;
        public CellValidator(FunTable funTable, SchemaReader reader, RolapCube cube) {
            super(funTable);
            this.reader = reader;
            final Statement statement =
                reader.getSchema().getInternalConnection().getInternalStatement();
            this.query =
                new Query(
                    statement,
                    cube,
                    new Formula[] {},
                    new QueryAxis[0],
                    null,
                    new QueryPart[0],
                    Collections.<Parameter>emptyList(),
                    false);
        }
        public Query getQuery() {
            return query;
        }
        public SchemaReader getSchemaReader() {
            return reader;
        }
        protected void defineParameter( Parameter param ) {        
        }
    }

    /**
     * This struct contains the necessary info for processing cell calculations.
     */
    public static class CellCalc {
        Exp cubeExp;
        Exp cubeExps[];
        Exp cellExp;
        int solve_order;
    }
}
