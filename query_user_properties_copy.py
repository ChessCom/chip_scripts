import json
import os

from chesscom.game_gateway.v1.game_gateway_service_pb2 import (
    Criteria,
    HydrateGamesByCriteriaRequest,
    SortDirection,
    SortField,
)
from chesscom.game_gateway.v1.game_gateway_service_twirp import GameGatewayServiceClient
from chesscom.game_storage.v1.game_stored_pb2 import TimeClass, Variant
from chesscom.user_properties.v1.user_properties_service_pb2 import (
    SearchUserPropertiesRequest,
    SearchUserPropertiesResponse,
    UserProperties,
    UserPropertiesPropertiesData,
)
from chesscom.user_properties.v1.user_properties_service_twirp import (
    UserPropertiesServiceClient,
)
from chesscom.user_targeting.v1.user_targeting_pb2 import (
    BinaryOperator,
    Criterion,
    LogicalOperator,
)

# Inject API keys
from dotenv import load_dotenv
from twirp.context import Context
from twirp.exceptions import TwirpServerException

load_dotenv()

agi_client = GameGatewayServiceClient(
    "https://services.nex.va-prod-01.chess-platform.com"
)
user_properties_client = UserPropertiesServiceClient("https://prod.chess-platform.com")

RATING_MIN = 1000
RATING_MAX = 2000
PLAYER_PER_BUCKET = 200
GAME_PER_PLAYER = 100


def fetch_players_per_bucket(rating_min: int, rating_max: int):
    try:
        try:
            user_properties_api_key = os.environ["USER_PROPERTIES_API_KEY"]
        except KeyError:
            raise RuntimeError(
                "USER_PROPERTIES_API_KEY environment variable is not set yet. To get the API key in Vault, "
                "go to chess-prod > platform > user-properties"
            )

        response = user_properties_client.SearchUserProperties(
            ctx=Context(),
            request=SearchUserPropertiesRequest(
                criteria=[
                    Criterion(
                        user_property="username",
                        binary_operator=BinaryOperator.EQUALS,
                        value="MangosCarloson",
                    ),
                ],
                # properties = ["username", 'player_id'],
                logical_operator=LogicalOperator.AND,
                limit=PLAYER_PER_BUCKET,
            ),
            headers={"X-Api-Key": user_properties_api_key},
            server_path_prefix="/service/user-properties",
        )

        print(response)
        print(type(response))
        # output = list(response.user_properties)
        # print(output[0])

        return [
            sample.properties["username"].value for sample in response.user_properties
        ]
    except TwirpServerException as e:
        print(e.code, e.message, e.meta, e.to_dict())


def fetch_games_per_player(player_id: str):
    try:
        try:
            agi_api_key = os.environ["AGI_API_KEY"]
        except KeyError:
            raise RuntimeError(
                "AGI_API_KEY environment variable is not set yet. To get the API key in Vault, "
                "go to cluster-chess-prod-va-01 > platform > foundation > game-archive"
            )

        response = agi_client.HydrateGamesByCriteria(
            ctx=Context(),
            request=HydrateGamesByCriteriaRequest(
                criteria=Criteria(
                    player_id=player_id,
                    time_classes=[TimeClass.TIME_CLASS_BLITZ],
                    # ply_from = 2,
                    rated=True,
                    variants=[Variant.VARIANT_CHESS],
                    sort_fields=[SortField.SORT_FIELD_GAME_END_TIME],
                    sort_direction=SortDirection.SORT_DIRECTION_DESC,
                    page=1,
                    page_size=GAME_PER_PLAYER,
                ),
                field_mask={"paths": ["eco_metadata"]},
            ),
            headers={"X-Api-Key": agi_api_key},
            server_path_prefix="/service/player-game-archive",
        )
        return list(response.hydrated_games)
    except TwirpServerException as e:
        print(e.code, e.message, e.meta, e.to_dict())


players = fetch_players_per_bucket(RATING_MIN, RATING_MAX)
print(players)
for player in players:
    games = fetch_games_per_player(player)
    print(len(games), player)
    if games:
        print(games[0])
    exit()

file_name = "users_" + str(RATING_MIN) + "_" + str(RATING_MAX) + ".json"

with open(file_name, "w") as file:
    json.dump(players, file, indent=4)
