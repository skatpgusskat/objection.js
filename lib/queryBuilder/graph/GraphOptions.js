const NO_RELATE = 'noRelate';
const NO_UNRELATE = 'noUnrelate';
const NO_INSERT = 'noInsert';
const NO_UPDATE = 'noUpdate';
const NO_DELETE = 'noDelete';

const UPDATE = 'update';
const RELATE = 'relate';
const UNRELATE = 'unrelate';
const INSERT_MISSING = 'insertMissing';

class GraphOptions {
  constructor(options) {
    this.options = options;
  }

  shouldRelate(node, currentGraph) {
    return !this.isRelateDisabled(node) && this._shouldRelate(node, currentGraph);
  }

  isRelateDisabled(node) {
    return this._hasOption(node, NO_RELATE);
  }

  shouldInsert(node, currentGraph) {
    return (
      !getCurrentNode(node, currentGraph) &&
      !this.isInsertDisabled(node) &&
      !this._shouldRelate(node, currentGraph) &&
      (!node.obj.$hasId() || this._hasOption(node, INSERT_MISSING))
    );
  }

  isInsertDisabled(node) {
    return this._hasOption(node, NO_INSERT);
  }

  shouldPatch(node, currentGraph) {
    return this._shouldPatch(node, currentGraph) && !this._hasOption(node, UPDATE);
  }

  shouldUpdate(node, currentGraph) {
    return this._shouldPatch(node, currentGraph) && this._hasOption(node, UPDATE);
  }

  shouldUnrelate(currentNode, graph) {
    return (
      !getNode(currentNode, graph) &&
      !this._shouldAncestorBeDeletedOrUnrelated(currentNode, graph) &&
      !this._hasOption(currentNode, NO_UNRELATE) &&
      this._shouldUnrelate(currentNode)
    );
  }

  shouldDelete(currentNode, graph) {
    return (
      !getNode(currentNode, graph) &&
      !this._shouldAncestorBeDeletedOrUnrelated(currentNode, graph) &&
      !this._hasOption(currentNode, NO_DELETE) &&
      !this._shouldUnrelate(currentNode)
    );
  }

  shouldInsertOrRelate(node, currentGraph) {
    return this.shouldInsert(node, currentGraph) || this.shouldRelate(node, currentGraph);
  }

  shouldDeleteOrUnrelate(currentNode, graph) {
    return this.shouldDelete(currentNode, graph) || this.shouldUnrelate(currentNode, graph);
  }

  shouldPatchOrUpdate(node, currentGraph) {
    return this.shouldPatch(node, currentGraph) || this.shouldUpdate(node, currentGraph);
  }

  _shouldPatch(node, currentGraph) {
    if (this.shouldRelate(node)) {
      // We should update all nodes that are going to be related. Note that
      // we don't actually update anything unless there is something to update
      // so this is just a preliminary test.
      return true;
    }

    return !!getCurrentNode(node, currentGraph) && !this._hasOption(node, NO_UPDATE);
  }

  _shouldRelate(node, currentGraph = null) {
    if (node.isReference || node.isDbReference) {
      return true;
    }

    return (
      !getCurrentNode(node, currentGraph) &&
      this._hasOption(node, RELATE) &&
      !!node.parentEdge &&
      !!node.parentEdge.relation &&
      node.parentEdge.relation.hasRelateProp(node.obj)
    );
  }

  _shouldUnrelate(currentNode) {
    return this._hasOption(currentNode, UNRELATE);
  }

  _shouldAncestorBeDeletedOrUnrelated(currentNode, graph) {
    if (!currentNode.parentNode) {
      return false;
    }

    return (
      !getNode(currentNode.parentNode, graph) ||
      this._shouldAncestorBeDeletedOrUnrelated(currentNode.parentNode, graph)
    );
  }

  _hasOption(node, optionName) {
    const option = this.options[optionName];

    if (Array.isArray(option)) {
      return option.indexOf(node.relationPathKey) !== -1;
    } else {
      return !!option;
    }
  }
}

function getCurrentNode(node, currentGraph) {
  if (!currentGraph || !node) {
    return null;
  }

  return currentGraph.nodeForNode(node);
}

function getNode(currentNode, graph) {
  if (!graph || !currentNode) {
    return null;
  }

  return graph.nodeForNode(currentNode);
}

module.exports = {
  GraphOptions
};
