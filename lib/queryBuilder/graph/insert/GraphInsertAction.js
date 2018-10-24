const { GraphAction } = require('../GraphAction');
const { groupBy, chunk, get, set } = require('../../../utils/objectUtils');
const { ModelGraphEdge } = require('../../../model/graph/ModelGraphEdge');
const { isTempColumn } = require('../../../utils/tmpColumnUtils');
const promiseUtils = require('../../../utils/promiseUtils');

/**
 * Inserts a batch of nodes for a GraphInsert.
 *
 * One of these is created for each batch of nodes that can be inserted at once.
 * However, the nodes can have a different table and not all databases support
 * batch inserts, so this class splits the inserts into further sub batches
 * when needed.
 */
class GraphInsertAction extends GraphAction {
  constructor({ nodes, currentGraph, dependents, graphOptions }) {
    super();

    // Nodes to insert.
    this.nodes = nodes;
    this.currentGraph = currentGraph;
    this.dependents = dependents;
    this.graphOptions = graphOptions;
  }

  run(builder) {
    const batches = this._createInsertBatches(builder);
    const concurrency = this._getConcurrency(builder, this.nodes);

    return promiseUtils.map(batches, batch => this._insertBatch(builder, batch), { concurrency });
  }

  _createInsertBatches(builder) {
    const batches = [];
    const batchSize = this._getBatchSize(builder);
    const nodesByModelClass = groupBy(this.nodes, getModelClass);

    for (const nodes of nodesByModelClass.values()) {
      for (const nodeBatch of chunk(nodes, batchSize)) {
        batches.push(nodeBatch);
      }
    }

    return batches;
  }

  _insertBatch(parentBuilder, nodes) {
    return this._beforeInsert(parentBuilder, nodes)
      .then(() => this._insert(parentBuilder, nodes))
      .then(() => this._afterInsert(nodes));
  }

  _beforeInsert(parentBuilder, nodes) {
    this._omitManyToManyExtraProps(nodes);
    this._copyValuesFromCurrentGraph(nodes);

    return this._runRelationBeforeInsertMethods(parentBuilder, nodes);
  }

  _omitManyToManyExtraProps(nodes) {
    for (const node of nodes) {
      if (
        node.parentEdge &&
        node.parentEdge.type === ModelGraphEdge.Type.Relation &&
        node.parentEdge.relation.joinTableExtras.length > 0
      ) {
        node.parentEdge.relation.omitExtraProps([node.obj]);
      }
    }
  }

  _copyValuesFromCurrentGraph(nodes) {
    for (const node of nodes) {
      const currentNode = this.currentGraph.nodeForNode(node);

      if (currentNode) {
        for (const prop of Object.keys(currentNode.obj)) {
          if (!node.obj.hasOwnProperty(prop) && !isTempColumn(prop)) {
            node.obj[prop] = currentNode.obj[prop];
          }
        }
      }
    }
  }

  _runRelationBeforeInsertMethods(parentBuilder, nodes) {
    return Promise.all(
      nodes.map(node => {
        if (node.parentEdge) {
          return node.parentEdge.relation.beforeInsert(node.obj, parentBuilder.context());
        } else {
          return null;
        }
      })
    );
  }

  _insert(parentBuilder, nodes) {
    const [{ modelClass }] = nodes;

    nodes = nodes.filter(node => {
      return this.graphOptions.shouldInsert(node, this.currentGraph);
    });

    for (const node of nodes) {
      delete node.obj[modelClass.uidProp];
      node.obj.$validate(null, { dataPath: node.dataPathKey });
    }

    if (nodes.length === 0) {
      return;
    }

    for (const node of nodes) {
      node.userData.inserted = true;
    }

    return modelClass
      .query()
      .insert(nodes.map(node => node.obj))
      .childQueryOf(parentBuilder)
      .copyFrom(parentBuilder, GraphAction.ReturningSelector)
      .execute();
  }

  _afterInsert(nodes) {
    for (const node of nodes) {
      this._resolveDependencies(node);
    }
  }

  _resolveDependencies(node) {
    const edges = this.dependents.get(node);

    if (edges) {
      for (const edge of edges) {
        this._resolveDependency(node, edge);
      }
    }
  }

  _resolveDependency(node, edge) {
    if (edge.type === ModelGraphEdge.Type.Relation && !edge.relation.joinTable) {
      this._resolveRelationDependency(node, edge);
    } else if (edge.refType === ModelGraphEdge.ReferenceType.Property) {
      this._resolvePropertyReferenceNode(node, edge);
    }
  }

  _resolveRelationDependency(node, edge) {
    const dependentNode = edge.getOtherNode(node);

    let sourceProp;
    let targetProp;

    if (edge.isOwnerNode(node)) {
      sourceProp = edge.relation.ownerProp;
      targetProp = edge.relation.relatedProp;
    } else {
      targetProp = edge.relation.ownerProp;
      sourceProp = edge.relation.relatedProp;
    }

    this._resolveReferences(node);

    for (let i = 0, l = targetProp.size; i < l; ++i) {
      targetProp.setProp(dependentNode.obj, i, sourceProp.getProp(node.obj, i));
    }

    for (const refNode of node.referencingNodes) {
      this._resolveDependency(refNode, refNode.parentEdge);
    }
  }

  _resolvePropertyReferenceNode(node, edge) {
    const dependentNode = edge.getOtherNode(node);

    let sourcePath;
    let targetPath;

    if (edge.isOwnerNode(node)) {
      sourcePath = edge.refOwnerDataPath;
      targetPath = edge.refRelatedDataPath;
    } else {
      targetPath = edge.refOwnerDataPath;
      sourcePath = edge.refRelatedDataPath;
    }

    const sourceValue = get(node.obj, sourcePath);
    const targetValue = get(dependentNode.obj, targetPath);

    if (targetValue === edge.refMatch) {
      set(dependentNode.obj, targetPath, sourceValue);
    } else {
      set(dependentNode.obj, targetPath, targetValue.replace(edge.refMatch, sourceValue));
    }
  }
}

function getModelClass(node) {
  return node.modelClass;
}

module.exports = {
  GraphInsertAction
};
