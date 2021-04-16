# Process Recipe Action

This actions purpose is to take a Recipe contributed via a PR, convert it to a Prefect Flow, register it with the Bakery specified in its metadata file, and run a test invocation of the Flow with a subset of the input data.

## Inputs

### `path_to_recipe_py`

**Required** The path to the `recipe.py` file within the PR. This is relative to the root of the repository.

### `path_to_meta_yaml`

**Required** The path to the `meta.yaml` file within the PR. This is relative to the root of the repository.


## Outputs

N/A

## Example usage

```yaml
# If using this recipe within the pangeo-forge/staged-recipes repository
uses: ./.github/actions/process_recipe
with:
    path_to_recipe_py: "recipes/my_recipe/recipe.py"
    path_to_meta_yaml: "recipes/my_recipe/meta.yaml"

# If using this recipe in any other repository
uses: pangeo-forge/staged-recipes/.github/actions/process_recipe@master
with:
    path_to_recipe_py: "recipes/my_recipe/recipe.py"
    path_to_meta_yaml: "recipes/my_recipe/meta.yaml"
```
