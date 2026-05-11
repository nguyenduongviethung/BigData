import chess
import chess.pgn
import numpy as np

def build_action_space():
    """
    Tạo một từ điển ánh xạ toàn bộ các nước đi hợp lệ (dưới dạng UCI) thành một số nguyên ID.
    Ví dụ: 'e2e4' -> 1045
    """
    uci_to_index = {}
    index = 0
    
    # 1. Các nước đi bình thường (Từ ô này sang ô khác)
    for from_sq in range(64):
        for to_sq in range(64):
            if from_sq != to_sq:
                move = chess.Move(from_sq, to_sq)
                uci_to_index[move.uci()] = index
                index += 1
                
    # 2. Các nước phong cấp (Promotions)
    # Ví dụ: a7a8q, a7b8n, v.v.
    promotion_pieces = [chess.QUEEN, chess.ROOK, chess.BISHOP, chess.KNIGHT]
    for from_sq in range(64):
        rank = chess.square_rank(from_sq)
        # Tốt chỉ phong cấp khi ở hàng 7 (Trắng) hoặc hàng 2 (Đen)
        if rank == 6 or rank == 1:
            for to_sq in range(64):
                # Tốt chỉ di chuyển sang các ô lân cận hoặc tiến thẳng
                if abs(from_sq - to_sq) in [7, 8, 9]:
                    for piece in promotion_pieces:
                        move = chess.Move(from_sq, to_sq, promotion=piece)
                        # Nếu nước đi chưa có trong dict thì thêm vào
                        if move.uci() not in uci_to_index:
                            uci_to_index[move.uci()] = index
                            index += 1
                            
    return uci_to_index

# Khởi tạo action space (Biến toàn cục để dùng lại nhiều lần, tránh tính toán lại)
ACTION_SPACE = build_action_space()
ACTION_SPACE_SIZE = len(ACTION_SPACE)

def board_to_tensor(board: chess.Board) -> np.ndarray:
    """
    Chuyển đổi trạng thái bàn cờ hiện tại thành Tensor (12, 8, 8).
    Sử dụng kiểu int8 để tối ưu tối đa dung lượng RAM/Storage.
    """
    # Khởi tạo tensor toàn số 0
    tensor = np.zeros((12, 8, 8), dtype=np.int8)

    # Lấy danh sách tất cả các quân cờ đang có trên bàn
    for square, piece in board.piece_map().items():
        # Lấy tọa độ hàng (rank) và cột (file) từ 0-7
        rank = chess.square_rank(square)
        file = chess.square_file(square)

        # Xác định channel (0 đến 5 cho Trắng)
        # piece_type trong python-chess đi từ 1 (Pawn) đến 6 (King)
        channel = piece.piece_type - 1

        # Nếu là quân Đen, tịnh tiến lên 6 channels tiếp theo (6 đến 11)
        if piece.color == chess.BLACK:
            channel += 6

        # Đánh dấu sự xuất hiện của quân cờ (One-hot encoding)
        tensor[channel, rank, file] = 1

    return tensor

def process_game_to_training_data(game: chess.pgn.Game):
    """
    Hàm này duyệt qua toàn bộ một ván cờ và trả về danh sách các (State, Policy, Value).
    """
    board = game.board()
    
    # Xác định nhãn Value (1: Trắng thắng, -1: Đen thắng, 0: Hòa)
    result_str = game.headers.get("Result", "*")
    if result_str == "1-0":
        value_label = 1.0
    elif result_str == "0-1":
        value_label = -1.0
    else:
        value_label = 0.0

    training_samples = []

    # Duyệt qua từng nước đi trong ván
    for move in game.mainline_moves():
        # 1. Trích xuất Tensor TRƯỚC khi đi nước cờ (Input X)
        state_tensor = board_to_tensor(board)
        
        # 2. Lấy Policy Label (Target Y1: Xác suất nước đi)
        move_uci = move.uci()
        # Ánh xạ nước cờ thành 1 số nguyên (ví dụ: 'e2e4' -> 1542)
        policy_label = ACTION_SPACE.get(move_uci, -1) 
        
        if policy_label != -1:
            # 3. Lưu lại mẫu dữ liệu (Một record chứa đủ X, Y1, Y2)
            training_samples.append({
                "state": state_tensor,                 # Input: Ma trận 12x8x8
                "policy": policy_label,                # Output 1: Số nguyên đại diện cho nước đi
                "value": value_label if board.turn == chess.WHITE else -value_label # Output 2: Dự đoán thắng/thua
            })

        # Thực hiện nước đi trên bàn cờ ảo để qua state tiếp theo
        board.push(move)

    return training_samples

# --- ĐOẠN TEST NHANH ---
if __name__ == "__main__":
    import io
    print(f"🌍 Kích thước không gian hành động: {ACTION_SPACE_SIZE} nước đi khả thi.")
    
    # Một ván cờ ngắn
    pgn = io.StringIO("1. e4 e5 2. Nf3 Nc6 3. Bb5 a6")
    game = chess.pgn.read_game(pgn)
    
    samples = process_game_to_training_data(game)
    print(f"Trích xuất thành công {len(samples)} trạng thái.")
    print("Mẫu dữ liệu đầu tiên (Trắng đi e4):")
    print(f"- Shape của State (Input): {samples[0]['state'].shape}")
    print(f"- Policy ID (Nước đi 'e2e4'): {samples[0]['policy']}")
    print(f"- Value (Kết quả từ góc nhìn Trắng): {samples[0]['value']}")