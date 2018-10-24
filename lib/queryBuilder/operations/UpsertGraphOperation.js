'use strict';

const QueryBuilderOperation = require('./QueryBuilderOperation');

const { ModelGraph } = require('../../model/graph/ModelGraph');
const { GraphOperation } = require('../graph/GraphOperation');
const { GraphInsert } = require('../graph/insert/GraphInsert');
const { GraphPatch } = require('../graph/patch/GraphPatch');
const { GraphDelete } = require('../graph/delete/GraphDelete');
const { GraphOptions } = require('../graph/GraphOptions');

class UpsertGraphOperation extends QueryBuilderOperation {
  constructor(name, opt) {
    super(
      name,
      Object.assign({}, opt, {
        opt: {}
      })
    );

    this.models = null;
    this.isArray = null;
    this.upsertOpt = opt.opt || {};
  }

  onAdd(builder, args) {
    const [objects] = args;

    this.isArray = Array.isArray(objects);
    this.models = builder.modelClass().ensureModelArray(objects, { skipValidation: true });

    // Never execute this builder.
    builder.resolve([]);
    return true;
  }

  onAfter1(builder) {
    const modelClass = builder.modelClass();
    const graphOptions = new GraphOptions(this.upsertOpt);
    const graph = ModelGraph.create(modelClass, this.models);

    return GraphOperation.fetchCurrentGraph({ builder, obj: this.models })
      .then(pruneGraphs(graph, graphOptions))
      .then(checkForErrors(graph, graphOptions))
      .then(executeOperations(graph, graphOptions, builder))
      .then(returnResult(this.models, this.isArray));
  }
}

function pruneGraphs(graph, graphOptions) {
  return currentGraph => {
    pruneDeletedBranches(graph, currentGraph, graphOptions);
    pruneRelatedBranches(graph, currentGraph, graphOptions);

    return currentGraph;
  };
}

function pruneDeletedBranches(graph, currentGraph, graphOptions) {}

function pruneRelatedBranches(graph, currentGraph, graphOptions) {}

function checkForErrors(graph, graphOptions) {
  return currentGraph => {
    for (const node of graph.nodes) {
      if (
        node.obj.$hasId() &&
        !graphOptions.shouldInsertOrRelate(node, currentGraph) &&
        !graphOptions.isInsertDisabled(node) &&
        !graphOptions.isRelateDisabled(node) &&
        !currentGraph.nodeForNode(node)
      ) {
        if (!node.parentNode) {
          throw new Error(
            `root model (id=${node.obj.$id()}) does not exist. If you want to insert it with an id, use the insertMissing option`
          );
        } else {
          throw new Error(
            `model (id=${node.obj.$id()}) is not a child of model (id=${node.parentNode.obj.$id()}). If you want to relate it, use the relate option. If you want to insert it with an id, use the insertMissing option`
          );
        }
      }
    }

    return currentGraph;
  };
}

function executeOperations(graph, graphOptions, builder) {
  return currentGraph => {
    return [GraphDelete, GraphInsert, GraphPatch].reduce((promise, Operation) => {
      const operation = new Operation({ graph, currentGraph, graphOptions });
      const actions = operation.createActions();

      return promise.then(() => executeActions(builder, actions));
    }, Promise.resolve());
  };
}

function executeActions(builder, actions) {
  return actions.reduce(
    (promise, action) => promise.then(() => action.run(builder)),
    Promise.resolve()
  );
}

function returnResult(models, isArray) {
  return () => (isArray ? models : models[0]);
}

module.exports = UpsertGraphOperation;
