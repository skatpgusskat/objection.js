const { JoinRowGraphInsertAction } = require('./JoinRowGraphInsertAction');
const { GraphInsertAction } = require('./GraphInsertAction');
const { GraphOperation } = require('../GraphOperation');
const { ModelGraphEdge } = require('../../../model/graph/ModelGraphEdge');

class GraphInsert extends GraphOperation {
  constructor(...args) {
    super(...args);

    this.dependencies = new Map();
    this.dependents = new Map();

    this._createDependencyMap();
  }

  createActions() {
    return [...this._createNormalActions(), ...this._createJoinRowActions()];
  }

  _createDependencyMap() {
    for (const edge of this.graph.edges) {
      if (edge.type == ModelGraphEdge.Type.Relation) {
        this._createRelationDependency(edge);
      } else {
        this._createReferenceDependency(edge);
      }
    }
  }

  _createRelationDependency(edge) {
    if (edge.relation.isObjectionHasManyRelation) {
      // In case of HasManyRelation the related node depends on the owner node
      // because the related node has the foreign key.
      this._addDependency(edge.relatedNode, edge);
    } else if (edge.relation.isObjectionBelongsToOneRelation) {
      // In case of BelongsToOneRelation the owner node depends on the related
      // node because the owner node has the foreign key.
      this._addDependency(edge.ownerNode, edge);
    }
  }

  _createReferenceDependency(edge) {
    this._addDependency(edge.ownerNode, edge);
  }

  _addDependency(node, edge) {
    this._addToMap(this.dependencies, node, edge);
    this._addToMap(this.dependents, edge.getOtherNode(node), edge);
  }

  _addToMap(map, node, edge) {
    let edges = map.get(node);

    if (!edges) {
      edges = [];
      map.set(node, edges);
    }

    edges.push(edge);
  }

  _createNormalActions() {
    const handledNodes = new Set();
    const actions = [];

    while (true) {
      // At this point, don't care if the nodes have already been inserted before
      // given to this class. `GraphInsertAction` will test that and only insert
      // new ones. We need to pass all nodes to `GraphInsertActions` so that we
      // can resolve all dependencies.
      const nodesToInsert = this.graph.nodes.filter(node => {
        return !this._isHandled(node, handledNodes) && !this._hasDependencies(node, handledNodes);
      });

      if (nodesToInsert.length === 0) {
        break;
      }

      actions.push(
        new GraphInsertAction({
          nodes: nodesToInsert,
          currentGraph: this.currentGraph,
          dependents: this.dependents,
          graphOptions: this.graphOptions
        })
      );

      for (const node of nodesToInsert) {
        this._markHandled(node, handledNodes);
      }
    }

    return actions;
  }

  _isHandled(node, handledNodes) {
    return handledNodes.has(node);
  }

  _hasDependencies(node, handledNodes) {
    if (!this.dependencies.has(node)) {
      return false;
    }

    for (const edge of this.dependencies.get(node)) {
      const dependencyNode = edge.getOtherNode(node);

      if (!handledNodes.has(dependencyNode)) {
        return true;
      }
    }

    return false;
  }

  _markHandled(node, handledNodes) {
    handledNodes.add(node);

    // The referencing nodes are all references that don't
    // represent any real entity. They are simply intermediate nodes
    // that depend on this node. Once this node is handled, we can
    // also mark those nodes as handled as there is nothing to actually
    // insert.
    for (const refNode of node.referencingNodes) {
      this._markHandled(refNode, handledNodes);
    }
  }

  _createJoinRowActions() {
    return [
      new JoinRowGraphInsertAction({
        nodes: this.graph.nodes.filter(node => {
          return (
            this.currentGraph.nodeForNode(node) === null &&
            node.parentEdge &&
            node.parentEdge.relation.isObjectionManyToManyRelation
          );
        }),

        currentGraph: this.currentGraph,
        graphOptions: this.graphOptions
      })
    ];
  }
}

module.exports = {
  GraphInsert
};
