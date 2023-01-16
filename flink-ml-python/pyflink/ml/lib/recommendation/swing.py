################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import typing

from pyflink.ml.core.param import Param, StringParam, IntParam, FloatParam, ParamValidators
from pyflink.ml.core.wrapper import JavaWithParams
from pyflink.ml.lib.param import HasOutputCol
from pyflink.ml.lib.recommendation.common import JavaRecommendationTransformer


class _SwingParams(
    JavaWithParams,
    HasOutputCol
):
    """
    Params for :class:`Swing`.
    """

    USER_COL: Param[str] = StringParam(
        "user_col",
        "Name of user column.",
        "user",
        ParamValidators.not_null())

    ITEM_COL: Param[str] = StringParam(
        "item_col",
        "Name of item column.",
        "item",
        ParamValidators.not_null())

    K: Param[int] = IntParam(
        "k",
<<<<<<< HEAD
        "The max number of related items for each item.",
        10,
=======
        "The max number of similar items to output for each item.",
        100,
>>>>>>> 22bca6f (Modify param names and description and simplified the code of swing.)
        ParamValidators.gt(0))

    MAX_USER_NUM_PER_ITEM: Param[int] = IntParam(
        "max_user_num_per_item",
        "The max number of users that has purchased for each item. If the number of users that have "
        + "purchased this item is larger than this value, then only maxUserNumPerItem users will "
        + "be sampled and used in the computation logic.",
        1000,
        ParamValidators.gt(0))

    MIN_USER_BEHAVIOR: Param[int] = IntParam(
        "min_user_behavior",
        "The min number of interaction behavior between item and user.",
        10,
        ParamValidators.gt(0))

    MAX_USER_BEHAVIOR: Param[int] = IntParam(
        "max_user_behavior",
        "The max number of interaction behavior between item and user."
        + "The algorithm filters out activate users.",
        1000,
        ParamValidators.gt(0))

    ALPHA1: Param[int] = IntParam(
        "alpha2",
        "This parameter is used to calculate weight of each user.",
        15,
        ParamValidators.gt_eq(0))

    ALPHA2: Param[int] = IntParam(
        "alpha2",
        "This parameter is used to calculate similarity of users.",
        0,
        ParamValidators.gt_eq(0))

    BETA: Param[float] = FloatParam(
        "beta",
        "This parameter is used to calculate weight of each user.",
        0.3,
        ParamValidators.gt_eq(0))

    def __init__(self, java_params):
        super(_SwingParams, self).__init__(java_params)

    def set_user_col(self, value: str):
        return typing.cast(_SwingParams, self.set(self.USER_COL, value))

    def get_user_col(self) -> str:
        return self.get(self.USER_COL)

    def set_item_col(self, value: str):
        return typing.cast(_SwingParams, self.set(self.ITEM_COL, value))

    def get_item_col(self) -> str:
        return self.get(self.ITEM_COL)

    def set_k(self, value: int):
        return typing.cast(_SwingParams, self.set(self.K, value))

    def get_k(self) -> int:
        return self.get(self.K)

    def set_max_user_num_per_item(self, value: int):
        return typing.cast(_SwingParams, self.set(self.MAX_USER_NUM_PER_ITEM, value))

    def get_max_user_num_per_item(self) -> int:
        return self.get(self.MAX_USER_NUM_PER_ITEM)

    def set_min_user_behavior(self, value: int):
        return typing.cast(_SwingParams, self.set(self.MIN_USER_BEHAVIOR, value))

    def get_min_user_behavior(self) -> int:
        return self.get(self.MIN_USER_BEHAVIOR)

    def set_max_user_behavior(self, value: int):
        return typing.cast(_SwingParams, self.set(self.MAX_USER_BEHAVIOR, value))

    def get_max_user_behavior(self) -> int:
        return self.get(self.MAX_USER_BEHAVIOR)

    def set_alpha1(self, value: int):
        return typing.cast(_SwingParams, self.set(self.ALPHA1, value))

    def get_alpha1(self) -> int:
        return self.get(self.ALPHA1)

    def set_alpha2(self, value: int):
        return typing.cast(_SwingParams, self.set(self.ALPHA2, value))

    def get_alpha2(self) -> int:
        return self.get(self.ALPHA2)

    def set_beta(self, value: float):
        return typing.cast(_SwingParams, self.set(self.BETA, value))

    def get_beta(self) -> float:
        return self.get(self.BETA)

    @property
    def user_col(self) -> str:
        return self.get_user_col()

    @property
    def item_col(self) -> str:
        return self.get_item_col()

    @property
    def k(self) -> int:
        return self.get_k()

    @property
    def max_user_num_per_item(self) -> int:
        return self.get_max_user_num_per_item()

    @property
    def min_user_behavior(self) -> int:
        return self.get_min_user_behavior()

    @property
    def max_user_behavior(self) -> int:
        return self.get_max_user_behavior()

    @property
    def alpha1(self) -> int:
        return self.get_alpha1()

    @property
    def alpha2(self) -> float:
        return self.get_alpha2()

    @property
    def beta(self) -> float:
        return self.get_beta()


class Swing(JavaRecommendationTransformer, _SwingParams):
    """
<<<<<<< HEAD
    An Estimator which implements the Swing algorithm.
=======
    An AlgoOperator which implements the Swing algorithm.
>>>>>>> 22bca6f (Modify param names and description and simplified the code of swing.)
    
    Swing is an item recall model. The topology of user-item graph usually can be described as
    user-item-user or item-user-item, which are like 'swing'. For example, if both user u
    and user v have purchased the same commodity i , they will form a relationship
    diagram similar to a swing. If u and v have purchased commodity j in
    addition to i, it is supposed i and j are similar. The formula of Swing is
    $$ w_{(i,j)}=\sum_{u\in U_i\cap U_j}\sum_{v\in U_i\cap U_j}{\frac{1}{{(I_u+\alpha_1)}^\beta}}*{\frac{1}{{(I_v+\alpha_1)}^\beta}}*{\frac{1}{\alpha\_2+|I_u\cap I_v|}} $$

    This implementation is based on the algorithm proposed in the paper: "Large Scale Product Graph Construction for
    Recommendation in E-commerce" by Xiaoyong Yang, Yadong Zhu and Yi Zhang. (https://arxiv.org/pdf/2010.05525.pdf)
    """

    def __init__(self, java_model=None):
        super(Swing, self).__init__(java_model)

    @classmethod
    def _java_transformer_package_name(cls) -> str:
        return "swing"

    @classmethod
    def _java_transformer_class_name(cls) -> str:
        return "Swing"
